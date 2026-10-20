package client

import (
	"fmt"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
)

// handleStoreSnapshot queues a detached requested full state before any
// concurrently buffered partials. No barrier or reset message is needed.
func (s *Streamer) handleStoreSnapshot(request *protocol.StoreSnapshotRequestPacket) error {
	if err := request.Validate(); err != nil {
		return err
	}
	s.relaySubscriptionMutex.Lock()
	if !s.onDemandStoreStreaming || s.relayStoreSubCount[request.StoreName] <= 0 {
		s.relaySubscriptionMutex.Unlock()
		return fmt.Errorf("snapshot requires an active on-demand subscription")
	}
	generation := s.relayStoreGeneration[request.StoreName] + 1
	if generation == 0 {
		s.relaySubscriptionMutex.Unlock()
		return fmt.Errorf("snapshot generation exhausted")
	}
	s.relayStoreGeneration[request.StoreName] = generation
	s.relayStoreCatchingUp[request.StoreName] = true
	delete(s.relayCatchupPartials, request.StoreName)
	s.relaySubscriptionMutex.Unlock()
	// Old debounced partials must not follow a newer detached baseline.
	s.discardGatheredPartial(request.StoreName)
	if err := s.sendStoreSnapshot(request.StoreName, generation, request.RequestID); err != nil {
		return err
	}

	s.relaySubscriptionMutex.Lock()
	defer s.relaySubscriptionMutex.Unlock()
	if s.relayStoreSubCount[request.StoreName] > 0 && s.relayStoreGeneration[request.StoreName] == generation {
		if partial := s.relayCatchupPartials[request.StoreName]; partial != nil {
			if err := s.sendPartialForGeneration(request.StoreName, partial, generation); err != nil {
				return err
			}
		}
		delete(s.relayCatchupPartials, request.StoreName)
		delete(s.relayStoreCatchingUp, request.StoreName)
	}
	return nil
}
