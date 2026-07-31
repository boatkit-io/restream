package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/boatkit-io/restream/pkg/restream"
)

const maxDeviceDataStreamLeases = 4096

// DataStreamEndpointAllocator is implemented by GoatStream (or another
// high-bandwidth service). It mints viewer-specific leases without owning
// Restream's device source refcount.
type DataStreamEndpointAllocator interface {
	AllocateViewer(
		ctx context.Context,
		sessionID string,
		deviceID string,
		subscription restream.DataStreamSubscription,
	) (restream.DataStreamEndpoint, error)
	// ReleaseViewer must be idempotent. A zero-expiry token remains valid only
	// while this parent lease is registered, so implementations must make
	// revocation durable enough for their own restart/failover model.
	ReleaseViewer(ctx context.Context, endpoint restream.DataStreamEndpoint) error
	// ReleaseSession revokes every remaining endpoint correlated with sessionID.
	// It must be idempotent.
	ReleaseSession(ctx context.Context, sessionID string) error
}

type allocatedDataStream struct {
	sessionID    string
	subscription restream.DataStreamSubscription
	endpoint     restream.DataStreamEndpoint
	released     bool
	stopped      bool
	closing      bool
	retrying     bool
	done         chan struct{}
}

// DeviceDataStreamBroker bridges authenticated viewer leases to one relay
// device. Each viewer gets its own endpoint, while Device forwards only
// aggregate stream activation and teardown.
type DeviceDataStreamBroker struct {
	device    *Device
	allocator DataStreamEndpointAllocator

	mu      sync.Mutex
	leases  map[string]*allocatedDataStream
	opening int
}

// NewDeviceDataStreamBroker creates a broker for one cloud relay device.
func NewDeviceDataStreamBroker(
	device *Device,
	allocator DataStreamEndpointAllocator,
) *DeviceDataStreamBroker {
	return &DeviceDataStreamBroker{
		device:    device,
		allocator: allocator,
		leases:    map[string]*allocatedDataStream{},
	}
}

// Open implements restream.DataStreamBroker.
func (b *DeviceDataStreamBroker) Open(
	ctx context.Context,
	subscription restream.DataStreamSubscription,
	accessLevel restream.AccessLevel,
) (restream.DataStreamEndpoint, error) {
	return b.open(ctx, "", subscription, accessLevel)
}

// OpenForSession implements restream.SessionDataStreamBroker.
func (b *DeviceDataStreamBroker) OpenForSession(
	ctx context.Context,
	sessionID string,
	subscription restream.DataStreamSubscription,
	accessLevel restream.AccessLevel,
) (restream.DataStreamEndpoint, error) {
	if sessionID == "" {
		return restream.DataStreamEndpoint{}, fmt.Errorf("data stream session ID is empty")
	}
	return b.open(ctx, sessionID, subscription, accessLevel)
}

func (b *DeviceDataStreamBroker) open(
	ctx context.Context,
	sessionID string,
	subscription restream.DataStreamSubscription,
	accessLevel restream.AccessLevel,
) (restream.DataStreamEndpoint, error) {
	if b == nil || b.device == nil || b.allocator == nil {
		return restream.DataStreamEndpoint{}, fmt.Errorf("data stream broker is not configured")
	}
	if err := subscription.Validate(); err != nil {
		return restream.DataStreamEndpoint{}, err
	}
	if b.device.StoreRegistry == nil {
		return restream.DataStreamEndpoint{}, fmt.Errorf("data stream device has no store registry")
	}
	if err := b.device.StoreRegistry.CheckStoreAccess(subscription.StoreName, accessLevel); err != nil {
		return restream.DataStreamEndpoint{}, err
	}
	b.mu.Lock()
	if len(b.leases)+b.opening >= maxDeviceDataStreamLeases {
		b.mu.Unlock()
		return restream.DataStreamEndpoint{}, fmt.Errorf("too many active data stream leases")
	}
	b.opening++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.opening--
		b.mu.Unlock()
	}()

	endpoint, err := b.allocator.AllocateViewer(
		ctx,
		sessionID,
		b.device.DeviceID,
		subscription,
	)
	if err != nil {
		return restream.DataStreamEndpoint{}, err
	}
	if endpoint.LeaseID == "" || endpoint.URL == "" || endpoint.Token == "" {
		_ = b.allocator.ReleaseViewer(ctx, endpoint)
		return restream.DataStreamEndpoint{}, fmt.Errorf("data stream allocator returned an incomplete endpoint")
	}

	// Allocate the viewer lease before asking the device to start. The endpoint
	// is not disclosed to the viewer until Open returns, but an in-process data
	// plane can already buffer the source's first recovery frame while startup
	// is being acknowledged. This avoids making every new viewer wait for the
	// next video keyframe.
	//
	// Source transitions have their own bounded acknowledgement timeout. Do not
	// let a viewer cancellation abandon an already queued device-side startup;
	// a canceled open below rolls the completed startup back normally.
	if err := b.device.ListeningToDataStream(
		context.Background(),
		subscription,
		accessLevel,
	); err != nil {
		_ = b.allocator.ReleaseViewer(context.Background(), endpoint)
		return restream.DataStreamEndpoint{}, err
	}

	b.mu.Lock()
	if _, exists := b.leases[endpoint.LeaseID]; exists {
		b.mu.Unlock()
		b.stopSourceOrRetry(subscription)
		_ = b.allocator.ReleaseViewer(ctx, endpoint)
		return restream.DataStreamEndpoint{}, fmt.Errorf(
			"data stream allocator reused active lease ID %q",
			endpoint.LeaseID,
		)
	}
	b.leases[endpoint.LeaseID] = &allocatedDataStream{
		sessionID:    sessionID,
		subscription: subscription,
		endpoint:     endpoint,
	}
	b.mu.Unlock()
	return endpoint, nil
}

// CloseSession implements restream.SessionDataStreamBroker.
func (b *DeviceDataStreamBroker) CloseSession(ctx context.Context, sessionID string) error {
	if b == nil || b.allocator == nil || sessionID == "" {
		return nil
	}
	b.mu.Lock()
	leaseIDs := make([]string, 0)
	for leaseID, allocated := range b.leases {
		if allocated.sessionID == sessionID {
			leaseIDs = append(leaseIDs, leaseID)
		}
	}
	b.mu.Unlock()

	var closeErr error
	for _, leaseID := range leaseIDs {
		if err := b.closeLease(ctx, leaseID); err != nil {
			closeErr = errors.Join(closeErr, err)
			b.startCloseRetry(leaseID)
		}
	}
	return errors.Join(closeErr, b.allocator.ReleaseSession(ctx, sessionID))
}

func (b *DeviceDataStreamBroker) stopSourceOrRetry(
	subscription restream.DataStreamSubscription,
) {
	stopCtx, cancel := context.WithTimeout(context.Background(), defaultDataStreamTransitionTimeout)
	err := b.device.StopListeningToDataStream(stopCtx, subscription)
	cancel()
	if err == nil {
		return
	}
	go func() {
		delay := time.Second
		for {
			timer := time.NewTimer(delay)
			<-timer.C
			retryCtx, retryCancel := context.WithTimeout(
				context.Background(),
				defaultDataStreamTransitionTimeout,
			)
			retryErr := b.device.StopListeningToDataStream(retryCtx, subscription)
			retryCancel()
			if retryErr == nil {
				return
			}
			if delay < time.Minute {
				delay *= 2
				if delay > time.Minute {
					delay = time.Minute
				}
			}
		}
	}()
}

// Close implements restream.DataStreamBroker.
func (b *DeviceDataStreamBroker) Close(
	ctx context.Context,
	endpoint restream.DataStreamEndpoint,
) error {
	if b == nil || b.device == nil || b.allocator == nil {
		return fmt.Errorf("data stream broker is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := b.closeLease(ctx, endpoint.LeaseID)
	if err != nil {
		b.startCloseRetry(endpoint.LeaseID)
	}
	return err
}

func (b *DeviceDataStreamBroker) closeLease(ctx context.Context, leaseID string) error {
	for {
		b.mu.Lock()
		allocated := b.leases[leaseID]
		if allocated == nil {
			b.mu.Unlock()
			return nil
		}
		if allocated.closing {
			done := allocated.done
			b.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		allocated.closing = true
		allocated.done = make(chan struct{})
		released := allocated.released
		stopped := allocated.stopped
		endpoint := allocated.endpoint
		subscription := allocated.subscription
		b.mu.Unlock()

		var releaseErr error
		if !released {
			releaseErr = b.allocator.ReleaseViewer(ctx, endpoint)
			released = releaseErr == nil
		}
		var stopErr error
		if !stopped {
			stopErr = b.device.StopListeningToDataStream(ctx, subscription)
			stopped = stopErr == nil
		}

		b.mu.Lock()
		allocated = b.leases[leaseID]
		if allocated != nil {
			allocated.released = released
			allocated.stopped = stopped
			allocated.closing = false
			close(allocated.done)
			allocated.done = nil
			if released && stopped {
				delete(b.leases, leaseID)
			}
		}
		b.mu.Unlock()
		return errors.Join(releaseErr, stopErr)
	}
}

func (b *DeviceDataStreamBroker) startCloseRetry(leaseID string) {
	b.mu.Lock()
	allocated := b.leases[leaseID]
	if allocated == nil || allocated.retrying {
		b.mu.Unlock()
		return
	}
	allocated.retrying = true
	b.mu.Unlock()

	go func() {
		delay := time.Second
		for {
			timer := time.NewTimer(delay)
			<-timer.C
			retryCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := b.closeLease(retryCtx, leaseID)
			cancel()
			if err == nil {
				return
			}
			if delay < time.Minute {
				delay *= 2
				if delay > time.Minute {
					delay = time.Minute
				}
			}
		}
	}()
}

var _ restream.DataStreamBroker = (*DeviceDataStreamBroker)(nil)
var _ restream.SessionDataStreamBroker = (*DeviceDataStreamBroker)(nil)
