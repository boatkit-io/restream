package restream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DataStreamSubscription identifies one high-bandwidth, store-owned stream.
//
// Restream transports this identity and its subscription lifecycle on the
// control plane. Stream payloads use a separate data-plane connection.
type DataStreamSubscription struct {
	StoreName  string
	StreamName string
	Key        string
}

// Validate checks that the subscription has a complete, bounded identity.
func (s DataStreamSubscription) Validate() error {
	switch {
	case strings.TrimSpace(s.StoreName) == "":
		return fmt.Errorf("data stream store name is required")
	case strings.TrimSpace(s.StreamName) == "":
		return fmt.Errorf("data stream name is required")
	case strings.TrimSpace(s.Key) == "":
		return fmt.Errorf("data stream key is required")
	case len(s.StoreName) > 256:
		return fmt.Errorf("data stream store name is too long")
	case len(s.StreamName) > 256:
		return fmt.Errorf("data stream name is too long")
	case len(s.Key) > 4096:
		return fmt.Errorf("data stream key is too long")
	default:
		return nil
	}
}

// DataPlaneStreamID returns the deterministic opaque stream identity used in
// high-bandwidth envelopes. Length-prefixing the control-plane fields keeps
// the hash unambiguous even when a source key contains separator characters.
func (s DataStreamSubscription) DataPlaneStreamID() string {
	identity := strconv.Itoa(len(s.StoreName)) + ":" + s.StoreName +
		strconv.Itoa(len(s.StreamName)) + ":" + s.StreamName +
		strconv.Itoa(len(s.Key)) + ":" + s.Key
	sum := sha256.Sum256([]byte(identity))
	return "restream:" + hex.EncodeToString(sum[:])
}

// DataStreamEndpoint is an authenticated, viewer-specific lease returned by a
// DataStreamBroker. Endpoint and Token are opaque to Restream.
type DataStreamEndpoint struct {
	LeaseID            string            `json:"leaseID"`
	URL                string            `json:"url"`
	Token              string            `json:"token"`
	ExpiresAtUnixMilli int64             `json:"expiresAtUnixMilli"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

// ExpiresAt returns an allocator-imposed endpoint expiry, or the zero time when
// the endpoint is scoped only to its authenticated parent subscription. Restream
// does not renew endpoint credentials; Close revokes them with the parent
// subscription.
func (e DataStreamEndpoint) ExpiresAt() time.Time {
	if e.ExpiresAtUnixMilli == 0 {
		return time.Time{}
	}
	return time.UnixMilli(e.ExpiresAtUnixMilli)
}

// DataStreamBroker allocates viewer endpoints while owning the aggregate
// source-subscription lifecycle. Implementations normally mint one viewer
// lease per Open call while refcounting a single device publisher.
type DataStreamBroker interface {
	Open(
		ctx context.Context,
		subscription DataStreamSubscription,
		accessLevel AccessLevel,
	) (DataStreamEndpoint, error)
	// Close revokes an endpoint and must be idempotent.
	Close(ctx context.Context, endpoint DataStreamEndpoint) error
}

// SessionDataStreamBroker additionally binds viewer leases to a Restream
// session correlation ID. The session ID is not an authentication credential;
// the viewer connection and eventual data-plane connection authenticate
// independently.
type SessionDataStreamBroker interface {
	DataStreamBroker
	OpenForSession(
		ctx context.Context,
		sessionID string,
		subscription DataStreamSubscription,
		accessLevel AccessLevel,
	) (DataStreamEndpoint, error)
	// CloseSession revokes every endpoint owned by sessionID and must be
	// idempotent.
	CloseSession(ctx context.Context, sessionID string) error
}

// DataStreamHandler starts or stops one exact keyed source. Implementations must
// honor ctx and make both transitions idempotent.
type DataStreamHandler func(ctx context.Context, key string, subscribe bool) error

type registeredDataStream struct {
	minAccessLevel AccessLevel
	handler        DataStreamHandler
}

type dataStreamRegistrationKey struct {
	storeName  string
	streamName string
}

// DataStreamDispatcher is the registration and authorization boundary for
// high-bandwidth streams. Like RPCDispatcher, each named operation owns an
// access level independently from its store's readable metadata.
type DataStreamDispatcher struct {
	mutex   sync.RWMutex
	streams map[dataStreamRegistrationKey]registeredDataStream
}

// NewDataStreamDispatcher creates an empty stream dispatcher.
func NewDataStreamDispatcher() *DataStreamDispatcher {
	return &DataStreamDispatcher{streams: map[dataStreamRegistrationKey]registeredDataStream{}}
}

// RegisterDataStream registers one store-owned stream name and its independent
// minimum access level.
func (d *DataStreamDispatcher) RegisterDataStream(
	storeName string,
	streamName string,
	minAccessLevel AccessLevel,
	handler DataStreamHandler,
) {
	if d == nil {
		panic("RegisterDataStream called on a nil dispatcher")
	}
	if strings.TrimSpace(storeName) == "" {
		panic("RegisterDataStream requires a store name")
	}
	if strings.TrimSpace(streamName) == "" {
		panic("RegisterDataStream requires a stream name")
	}
	if len(storeName) > 256 || len(streamName) > 256 {
		panic("RegisterDataStream name exceeds the protocol limit")
	}
	if handler == nil {
		panic("RegisterDataStream requires a handler for " + streamName)
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()
	registrationKey := dataStreamRegistrationKey{
		storeName:  storeName,
		streamName: streamName,
	}
	if _, exists := d.streams[registrationKey]; exists {
		panic("Double-registration of data stream: " + storeName + "/" + streamName)
	}
	d.streams[registrationKey] = registeredDataStream{
		minAccessLevel: minAccessLevel,
		handler:        handler,
	}
}

// HasRegistrations reports whether this dispatcher advertises data-stream
// support to a relay.
func (d *DataStreamDispatcher) HasRegistrations() bool {
	if d == nil {
		return false
	}
	d.mutex.RLock()
	has := len(d.streams) > 0
	d.mutex.RUnlock()
	return has
}

// CheckAccess verifies the exact registered store/stream pair and its
// independently configured access level.
func (d *DataStreamDispatcher) CheckAccess(
	subscription DataStreamSubscription,
	accessLevel AccessLevel,
) error {
	if err := subscription.Validate(); err != nil {
		return err
	}
	if d == nil {
		return fmt.Errorf("data stream dispatcher is not configured")
	}
	d.mutex.RLock()
	stream, exists := d.streams[dataStreamRegistrationKey{
		storeName:  subscription.StoreName,
		streamName: subscription.StreamName,
	}]
	d.mutex.RUnlock()
	if !exists {
		return fmt.Errorf(
			"data stream %q is not registered for store %q",
			subscription.StreamName,
			subscription.StoreName,
		)
	}
	if accessLevel < stream.minAccessLevel {
		return fmt.Errorf(
			"data stream %q called with insufficient access (%d < %d)",
			subscription.StreamName,
			accessLevel,
			stream.minAccessLevel,
		)
	}
	return nil
}

// Dispatch authorizes and invokes one registered stream transition.
func (d *DataStreamDispatcher) Dispatch(
	ctx context.Context,
	subscription DataStreamSubscription,
	accessLevel AccessLevel,
	subscribe bool,
) error {
	if err := d.CheckAccess(subscription, accessLevel); err != nil {
		return err
	}
	d.mutex.RLock()
	stream := d.streams[dataStreamRegistrationKey{
		storeName:  subscription.StoreName,
		streamName: subscription.StreamName,
	}]
	d.mutex.RUnlock()
	return stream.handler(ctx, subscription.Key, subscribe)
}
