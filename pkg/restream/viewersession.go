package restream

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	defaultViewerSessionDetachedTimeout    = 60 * time.Second
	defaultMaxViewerSessions               = 10000
	defaultMaxViewerSessionsPerOwner       = 64
	defaultMaxViewerSessionLifecycleEvents = 4096
	viewerSessionReapInterval              = time.Second
)

// ViewerSessionIdentity is the authenticated ownership and application scope
// of one resumable viewer session. Session IDs are correlation identifiers,
// not credentials; every attachment must authenticate independently and
// resolve to the same owner and scope.
type ViewerSessionIdentity struct {
	OwnerID     string
	ScopeID     string
	AccessLevel AccessLevel
}

func (i ViewerSessionIdentity) validate() error {
	if i.OwnerID == "" {
		return errors.New("viewer session owner ID is empty")
	}
	if len(i.OwnerID) > 1024 {
		return errors.New("viewer session owner ID is too long")
	}
	if i.ScopeID == "" {
		return errors.New("viewer session scope ID is empty")
	}
	if len(i.ScopeID) > 1024 {
		return errors.New("viewer session scope ID is too long")
	}
	return nil
}

func (i ViewerSessionIdentity) ownerKey() string {
	return i.ScopeID + "\x00" + i.OwnerID
}

// ViewerSessionIdentityLookupFunc resolves the freshly authenticated identity
// for each socket attachment.
type ViewerSessionIdentityLookupFunc func() (ViewerSessionIdentity, error)

// ViewerSessionLifecycleKind describes a session ownership transition.
type ViewerSessionLifecycleKind string

const (
	ViewerSessionCreated  ViewerSessionLifecycleKind = "created"
	ViewerSessionAttached ViewerSessionLifecycleKind = "attached"
	ViewerSessionDetached ViewerSessionLifecycleKind = "detached"
	ViewerSessionClosed   ViewerSessionLifecycleKind = "closed"
)

// ViewerSessionLifecycleEvent can be translated by GoatCloud into lifecycle
// events for a separately deployed GoatStream service.
type ViewerSessionLifecycleEvent struct {
	SessionID string
	Identity  ViewerSessionIdentity
	Kind      ViewerSessionLifecycleKind
	Reason    string
}

// ViewerSessionLifecycleHandler receives ordered lifecycle events on a
// dedicated bounded manager worker.
type ViewerSessionLifecycleHandler func(ViewerSessionLifecycleEvent)

// ViewerSessionManagerOptions configures bounded in-memory viewer sessions.
// Sessions intentionally do not survive a Restream server restart.
type ViewerSessionManagerOptions struct {
	DetachedTimeout     time.Duration
	MaxSessions         int
	MaxSessionsPerOwner int
	MaxLifecycleEvents  int
	LifecycleHandler    ViewerSessionLifecycleHandler
}

func (o ViewerSessionManagerOptions) withDefaults() ViewerSessionManagerOptions {
	if o.DetachedTimeout <= 0 {
		o.DetachedTimeout = defaultViewerSessionDetachedTimeout
	}
	if o.MaxSessions <= 0 {
		o.MaxSessions = defaultMaxViewerSessions
	}
	if o.MaxSessionsPerOwner <= 0 {
		o.MaxSessionsPerOwner = defaultMaxViewerSessionsPerOwner
	}
	if o.MaxLifecycleEvents <= 0 {
		o.MaxLifecycleEvents = defaultMaxViewerSessionLifecycleEvents
	}
	return o
}

type viewerSessionRecord struct {
	identity  ViewerSessionIdentity
	tracker   *socketTracker
	expiresAt time.Time
}

// ViewerSessionManager retains viewer state independently of transient
// Socket.IO connections.
type ViewerSessionManager struct {
	options ViewerSessionManagerOptions

	mu          sync.Mutex
	sessions    map[string]*viewerSessionRecord
	ownerCounts map[string]int
	closed      bool
	stop        chan struct{}
	done        chan struct{}
	cleanup     chan *socketTracker
	cleanupWG   sync.WaitGroup
	lifecycle   chan ViewerSessionLifecycleEvent
	lifecycleWG sync.WaitGroup
	closeOnce   sync.Once
}

// NewViewerSessionManager creates an in-memory session manager and starts its
// single expiration worker.
func NewViewerSessionManager(options ViewerSessionManagerOptions) *ViewerSessionManager {
	options = options.withDefaults()
	manager := &ViewerSessionManager{
		options:     options,
		sessions:    map[string]*viewerSessionRecord{},
		ownerCounts: map[string]int{},
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		cleanup:     make(chan *socketTracker, options.MaxSessions),
	}
	if options.LifecycleHandler != nil {
		manager.lifecycle = make(chan ViewerSessionLifecycleEvent, options.MaxLifecycleEvents)
		manager.lifecycleWG.Add(1)
		go manager.dispatchLifecycle()
	}
	for range 4 {
		manager.cleanupWG.Add(1)
		go manager.cleanupSessions()
	}
	go manager.reapExpired()
	return manager
}

// Close destroys every retained session and waits for the expiration worker.
func (m *ViewerSessionManager) Close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		close(m.stop)
		<-m.done

		m.mu.Lock()
		m.closed = true
		records := make([]*viewerSessionRecord, 0, len(m.sessions))
		for _, record := range m.sessions {
			records = append(records, record)
		}
		m.sessions = map[string]*viewerSessionRecord{}
		m.ownerCounts = map[string]int{}
		for _, record := range records {
			m.publishLifecycleLocked(ViewerSessionLifecycleEvent{
				SessionID: record.tracker.sessionID,
				Identity:  record.identity,
				Kind:      ViewerSessionClosed,
				Reason:    "manager shutdown",
			})
			m.cleanup <- record.tracker
		}
		m.mu.Unlock()
		if m.lifecycle != nil {
			close(m.lifecycle)
			m.lifecycleWG.Wait()
		}
		close(m.cleanup)
		m.cleanupWG.Wait()
	})
}

func (m *ViewerSessionManager) dispatchLifecycle() {
	defer m.lifecycleWG.Done()
	for event := range m.lifecycle {
		func() {
			defer func() {
				_ = recover()
			}()
			m.options.LifecycleHandler(event)
		}()
	}
}

func (m *ViewerSessionManager) publishLifecycleLocked(event ViewerSessionLifecycleEvent) {
	if m.lifecycle != nil {
		m.lifecycle <- event
	}
}

func (m *ViewerSessionManager) cleanupSessions() {
	defer m.cleanupWG.Done()
	for tracker := range m.cleanup {
		tracker.destroySession()
	}
}

func (m *ViewerSessionManager) reapExpired() {
	ticker := time.NewTicker(viewerSessionReapInterval)
	defer func() {
		ticker.Stop()
		close(m.done)
	}()
	for {
		select {
		case now := <-ticker.C:
			m.expireBefore(now)
		case <-m.stop:
			return
		}
	}
}

func (m *ViewerSessionManager) expireBefore(now time.Time) {
	m.mu.Lock()
	expired := make([]*viewerSessionRecord, 0)
	for sessionID, record := range m.sessions {
		if record.expiresAt.IsZero() || record.expiresAt.After(now) {
			continue
		}
		delete(m.sessions, sessionID)
		m.decrementOwnerLocked(record.identity)
		m.publishLifecycleLocked(ViewerSessionLifecycleEvent{
			SessionID: sessionID,
			Identity:  record.identity,
			Kind:      ViewerSessionClosed,
			Reason:    "detached timeout",
		})
		expired = append(expired, record)
	}
	m.mu.Unlock()

	for _, record := range expired {
		m.cleanup <- record.tracker
	}
}

func (m *ViewerSessionManager) decrementOwnerLocked(identity ViewerSessionIdentity) {
	ownerKey := identity.ownerKey()
	count := m.ownerCounts[ownerKey]
	if count <= 1 {
		delete(m.ownerCounts, ownerKey)
		return
	}
	m.ownerCounts[ownerKey] = count - 1
}

func (m *ViewerSessionManager) attach(
	request ViewerSessionAttachRequest,
	identity ViewerSessionIdentity,
	config socketTrackerConfig,
) (*socketTracker, bool, error) {
	if m == nil {
		return nil, false, errors.New("viewer session manager is nil")
	}
	if err := identity.validate(); err != nil {
		return nil, false, err
	}
	if len(request.SessionID) > 64 {
		return nil, false, errors.New("viewer session ID is too long")
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, false, errors.New("viewer session manager is closed")
	}

	if request.SessionID != "" {
		record := m.sessions[request.SessionID]
		if record == nil {
			m.mu.Unlock()
			return nil, false, errors.New("viewer session does not exist or has expired")
		}
		if record.identity.OwnerID != identity.OwnerID ||
			record.identity.ScopeID != identity.ScopeID {
			m.mu.Unlock()
			return nil, false, errors.New("viewer session belongs to a different authenticated identity")
		}
		if record.identity.AccessLevel != identity.AccessLevel {
			delete(m.sessions, request.SessionID)
			m.decrementOwnerLocked(record.identity)
			m.publishLifecycleLocked(ViewerSessionLifecycleEvent{
				SessionID: request.SessionID,
				Identity:  record.identity,
				Kind:      ViewerSessionClosed,
				Reason:    "authenticated access changed",
			})
			m.cleanup <- record.tracker
			m.mu.Unlock()
			return nil, false, errors.New("viewer session authorization changed")
		}
		if !record.tracker.isCompatibleSessionConfig(config) {
			m.mu.Unlock()
			return nil, false, errors.New("viewer session belongs to a different Restream scope")
		}
		record.expiresAt = time.Time{}
		tracker := record.tracker
		m.mu.Unlock()
		tracker.updateSessionConfig(config, identity)
		return tracker, true, nil
	}

	if len(m.sessions) >= m.options.MaxSessions {
		m.mu.Unlock()
		return nil, false, errors.New("viewer session limit reached")
	}
	ownerKey := identity.ownerKey()
	if m.ownerCounts[ownerKey] >= m.options.MaxSessionsPerOwner {
		m.mu.Unlock()
		return nil, false, errors.New("viewer session owner limit reached")
	}

	sessionID, err := newViewerSessionID()
	if err != nil {
		m.mu.Unlock()
		return nil, false, err
	}
	for m.sessions[sessionID] != nil {
		sessionID, err = newViewerSessionID()
		if err != nil {
			m.mu.Unlock()
			return nil, false, err
		}
	}

	tracker := newSocketTracker(config)
	tracker.sessionID = sessionID
	tracker.sessionManager = m
	tracker.sessionIdentity = identity
	m.sessions[sessionID] = &viewerSessionRecord{
		identity: identity,
		tracker:  tracker,
	}
	m.ownerCounts[ownerKey]++
	m.publishLifecycleLocked(ViewerSessionLifecycleEvent{
		SessionID: sessionID,
		Identity:  identity,
		Kind:      ViewerSessionCreated,
	})
	m.mu.Unlock()
	return tracker, false, nil
}

func (m *ViewerSessionManager) detach(
	sessionID string,
	tracker *socketTracker,
	generation uint64,
) {
	if m == nil || tracker == nil || !tracker.detachSocket(generation) {
		return
	}
	m.mu.Lock()
	record := m.sessions[sessionID]
	if record != nil && record.tracker == tracker {
		record.expiresAt = time.Now().Add(m.options.DetachedTimeout)
		m.publishLifecycleLocked(ViewerSessionLifecycleEvent{
			SessionID: sessionID,
			Identity:  record.identity,
			Kind:      ViewerSessionDetached,
		})
	}
	m.mu.Unlock()
}

func (m *ViewerSessionManager) closeSession(
	sessionID string,
	identity ViewerSessionIdentity,
) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	record := m.sessions[sessionID]
	if record == nil {
		m.mu.Unlock()
		return nil
	}
	if record.identity.OwnerID != identity.OwnerID ||
		record.identity.ScopeID != identity.ScopeID {
		m.mu.Unlock()
		return errors.New("viewer session belongs to a different authenticated identity")
	}
	delete(m.sessions, sessionID)
	m.decrementOwnerLocked(record.identity)
	m.publishLifecycleLocked(ViewerSessionLifecycleEvent{
		SessionID: sessionID,
		Identity:  record.identity,
		Kind:      ViewerSessionClosed,
		Reason:    "explicit close",
	})
	m.cleanup <- record.tracker
	m.mu.Unlock()
	return nil
}

func (m *ViewerSessionManager) abort(sessionID string, tracker *socketTracker) {
	if m == nil || tracker == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	record := m.sessions[sessionID]
	if record == nil || record.tracker != tracker {
		m.mu.Unlock()
		return
	}
	delete(m.sessions, sessionID)
	m.decrementOwnerLocked(record.identity)
	m.publishLifecycleLocked(ViewerSessionLifecycleEvent{
		SessionID: sessionID,
		Identity:  record.identity,
		Kind:      ViewerSessionClosed,
		Reason:    "session aborted",
	})
	m.cleanup <- tracker
	m.mu.Unlock()
}

func (m *ViewerSessionManager) markAttached(sessionID string, tracker *socketTracker) {
	if m == nil || tracker == nil {
		return
	}
	m.mu.Lock()
	record := m.sessions[sessionID]
	if record != nil && record.tracker == tracker {
		m.publishLifecycleLocked(ViewerSessionLifecycleEvent{
			SessionID: sessionID,
			Identity:  record.identity,
			Kind:      ViewerSessionAttached,
		})
	}
	m.mu.Unlock()
}

func newViewerSessionID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate viewer session ID: %w", err)
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80

	var encoded [36]byte
	hex.Encode(encoded[0:8], id[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], id[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], id[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], id[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], id[10:16])
	return string(encoded[:]), nil
}
