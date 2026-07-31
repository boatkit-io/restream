package datastream

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrQueueFull means a provisional unit could not fit in bounded memory.
	// The producer should submit the newest complete recovery Frame.
	ErrQueueFull = errors.New("data stream scheduler queue full")
	// ErrUnknownStream means the stream was not registered with the scheduler.
	ErrUnknownStream = errors.New("data stream scheduler stream is not registered")
	// ErrNeedsRecovery means a dependent unit was discarded because the stream
	// must first provide a discontinuity-marked recovery Frame.
	ErrNeedsRecovery = errors.New("data stream scheduler requires a recovery frame")
)

const defaultQuantumBytes = 16 * 1024

// StreamPolicy controls fairness and starvation prevention for one stream.
type StreamPolicy struct {
	// Weight controls the byte quantum relative to other streams. Values below
	// one are treated as one.
	Weight uint32
	// MaxSilence promotes a queued stream ahead of weighted fairness after it
	// has not emitted for this long.
	MaxSilence time.Duration
}

// SchedulerConfig bounds device-side multiplexing memory.
type SchedulerConfig struct {
	MaxPendingBytes int
	MaxPendingUnits int
	QuantumBytes    int
	Now             func() time.Time
}

type queuedEnvelope struct {
	envelope Envelope
	bytes    int
	queuedAt time.Time
}

type scheduledStream struct {
	policy        StreamPolicy
	queue         []queuedEnvelope
	deficit       int64
	lastDelivered time.Time
	needsRecovery bool
}

// Scheduler multiplexes independently droppable stream units without allowing
// a slow uplink to backpressure producers or ordinary Restream traffic.
type Scheduler struct {
	mu sync.Mutex

	maxPendingBytes int
	maxPendingUnits int
	quantumBytes    int
	now             func() time.Time

	streams      map[string]*scheduledStream
	order        []string
	cursor       int
	pendingBytes int
	pendingUnits int
	droppedUnits uint64
}

// NewScheduler creates a bounded high-bandwidth uplink scheduler.
func NewScheduler(config SchedulerConfig) *Scheduler {
	if config.MaxPendingBytes <= 0 {
		config.MaxPendingBytes = 8 * 1024 * 1024
	}
	if config.MaxPendingUnits <= 0 {
		config.MaxPendingUnits = 1024
	}
	if config.QuantumBytes <= 0 {
		config.QuantumBytes = defaultQuantumBytes
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Scheduler{
		maxPendingBytes: config.MaxPendingBytes,
		maxPendingUnits: config.MaxPendingUnits,
		quantumBytes:    config.QuantumBytes,
		now:             config.Now,
		streams:         map[string]*scheduledStream{},
	}
}

// Register adds or updates a stream's scheduling policy.
func (s *Scheduler) Register(streamID string, policy StreamPolicy) error {
	if streamID == "" {
		return fmt.Errorf("data stream ID is required")
	}
	if policy.Weight == 0 {
		policy.Weight = 1
	}
	s.mu.Lock()
	if stream := s.streams[streamID]; stream != nil {
		stream.policy = policy
		s.mu.Unlock()
		return nil
	}
	s.streams[streamID] = &scheduledStream{policy: policy}
	s.order = append(s.order, streamID)
	s.mu.Unlock()
	return nil
}

// Unregister drops pending work and removes a stream.
func (s *Scheduler) Unregister(streamID string) {
	s.mu.Lock()
	stream := s.streams[streamID]
	if stream == nil {
		s.mu.Unlock()
		return
	}
	s.dropQueueLocked(stream)
	delete(s.streams, streamID)
	for i, id := range s.order {
		if id == streamID {
			s.order = append(s.order[:i], s.order[i+1:]...)
			if len(s.order) == 0 {
				s.cursor = 0
			} else if s.cursor >= len(s.order) {
				s.cursor %= len(s.order)
			}
			break
		}
	}
	s.mu.Unlock()
}

// Submit queues a unit without blocking. A recovery Frame supersedes all
// pending work for its stream, which bounds latency after congestion.
func (s *Scheduler) Submit(envelope Envelope) error {
	if err := envelope.Validate(defaultMaxPayload); err != nil {
		return err
	}
	size := encodedSize(envelope)
	s.mu.Lock()
	defer s.mu.Unlock()

	stream := s.streams[envelope.StreamID]
	if stream == nil {
		return ErrUnknownStream
	}
	isRecovery := envelope.PayloadType == PayloadFrame &&
		envelope.Flags&FlagRecovery != 0
	if stream.needsRecovery {
		if !isRecovery || envelope.Flags&FlagDiscontinuity == 0 {
			s.droppedUnits++
			return ErrNeedsRecovery
		}
	}
	if isRecovery {
		s.dropQueueLocked(stream)
		stream.needsRecovery = false
	}
	if s.pendingUnits+1 > s.maxPendingUnits || s.pendingBytes+size > s.maxPendingBytes {
		// Once any atomic unit is skipped, later dependent frames or BlockSet
		// commits cannot safely continue until a fresh recovery Frame.
		stream.needsRecovery = true
		return ErrQueueFull
	}

	envelope.Payload = append([]byte(nil), envelope.Payload...)
	stream.queue = append(stream.queue, queuedEnvelope{
		envelope: envelope,
		bytes:    size,
		queuedAt: s.now(),
	})
	s.pendingUnits++
	s.pendingBytes += size
	return nil
}

// Next returns the next unit selected by starvation deadlines followed by
// weighted deficit round-robin.
func (s *Scheduler) Next() (Envelope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingUnits == 0 || len(s.order) == 0 {
		return Envelope{}, false
	}

	now := s.now()
	if stream := s.mostOverdueStreamLocked(now); stream != nil {
		return s.popLocked(stream, now), true
	}

	// Every pass adds one weighted quantum. Repeating in this call allows a
	// large atomic frame to accumulate enough deficit without busy-polling.
	for {
		for offset := 0; offset < len(s.order); offset++ {
			index := (s.cursor + offset) % len(s.order)
			stream := s.streams[s.order[index]]
			if stream == nil || len(stream.queue) == 0 {
				continue
			}
			stream.deficit += int64(s.quantumBytes) * int64(stream.policy.Weight)
			if int64(stream.queue[0].bytes) > stream.deficit {
				continue
			}
			stream.deficit -= int64(stream.queue[0].bytes)
			s.cursor = (index + 1) % len(s.order)
			return s.popLocked(stream, now), true
		}
	}
}

// NeedsRecovery reports whether a provisional BlockSet was rejected. It stays
// true until a recovery Frame is accepted or the stream is unregistered.
func (s *Scheduler) NeedsRecovery(streamID string) bool {
	s.mu.Lock()
	stream := s.streams[streamID]
	needsRecovery := stream != nil && stream.needsRecovery
	s.mu.Unlock()
	return needsRecovery
}

// Stats returns current bounded-queue accounting.
func (s *Scheduler) Stats() (pendingUnits int, pendingBytes int, droppedUnits uint64) {
	s.mu.Lock()
	pendingUnits = s.pendingUnits
	pendingBytes = s.pendingBytes
	droppedUnits = s.droppedUnits
	s.mu.Unlock()
	return
}

func (s *Scheduler) mostOverdueStreamLocked(now time.Time) *scheduledStream {
	var selected *scheduledStream
	var selectedOverdue time.Duration
	for _, streamID := range s.order {
		stream := s.streams[streamID]
		if stream == nil || len(stream.queue) == 0 || stream.policy.MaxSilence <= 0 {
			continue
		}
		since := stream.lastDelivered
		if since.IsZero() {
			since = stream.queue[0].queuedAt
		}
		overdue := now.Sub(since) - stream.policy.MaxSilence
		if overdue >= 0 && (selected == nil || overdue > selectedOverdue) {
			selected = stream
			selectedOverdue = overdue
		}
	}
	return selected
}

func (s *Scheduler) popLocked(stream *scheduledStream, now time.Time) Envelope {
	queued := stream.queue[0]
	stream.queue = stream.queue[1:]
	stream.lastDelivered = now
	s.pendingUnits--
	s.pendingBytes -= queued.bytes
	return queued.envelope
}

func (s *Scheduler) dropQueueLocked(stream *scheduledStream) {
	for _, queued := range stream.queue {
		s.pendingBytes -= queued.bytes
		s.pendingUnits--
		s.droppedUnits++
	}
	stream.queue = nil
	stream.deficit = 0
}

func encodedSize(envelope Envelope) int {
	return fixedHeaderSize + len(envelope.StreamID) + len(envelope.Format) + len(envelope.Payload)
}
