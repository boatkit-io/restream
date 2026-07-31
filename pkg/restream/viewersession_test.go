package restream

import (
	"regexp"
	"testing"
	"time"
)

func TestViewerSessionManagerCreatesAndResumesUUIDSession(t *testing.T) {
	manager := NewViewerSessionManager(ViewerSessionManagerOptions{
		DetachedTimeout: time.Minute,
	})
	t.Cleanup(manager.Close)
	identity := ViewerSessionIdentity{
		OwnerID:     "auth0|viewer-a",
		ScopeID:     "boat-a",
		AccessLevel: AccessLevel(2),
	}
	config := socketTrackerConfig{
		limits: SocketHandlerLimits{}.withDefaults(),
	}

	tracker, resumed, err := manager.attach(ViewerSessionAttachRequest{}, identity, config)
	if err != nil {
		t.Fatalf("fresh session attach failed: %v", err)
	}
	if resumed {
		t.Fatal("fresh session reported resumed")
	}
	if !regexp.MustCompile(
		`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
	).MatchString(tracker.sessionID) {
		t.Fatalf("session ID %q is not a UUIDv4", tracker.sessionID)
	}

	resumedTracker, resumed, err := manager.attach(ViewerSessionAttachRequest{
		SessionID: tracker.sessionID,
	}, identity, config)
	if err != nil {
		t.Fatalf("session resume failed: %v", err)
	}
	if !resumed || resumedTracker != tracker {
		t.Fatal("session resume did not retain the original tracker")
	}

	changedAccess := identity
	changedAccess.AccessLevel--
	if _, _, err := manager.attach(ViewerSessionAttachRequest{
		SessionID: tracker.sessionID,
	}, changedAccess, config); err == nil {
		t.Fatal("session resumed after its authenticated access level changed")
	}

	otherIdentity := identity
	otherIdentity.OwnerID = "auth0|viewer-b"
	if _, _, err := manager.attach(ViewerSessionAttachRequest{
		SessionID: tracker.sessionID,
	}, otherIdentity, config); err == nil {
		t.Fatal("different authenticated owner resumed the session")
	}
}

func TestViewerSessionDetachedStoreAccumulatorTransitions(t *testing.T) {
	store := NewRelayStore[
		viewerSocketTestState,
		*viewerSocketTestState,
		*viewerSocketTestPartial,
	](viewerSocketTestStoreName, &viewerSocketTestState{
		Values: map[string]int{},
	}, AccessLevelPublic)
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	manager := NewViewerSessionManager(ViewerSessionManagerOptions{})
	t.Cleanup(manager.Close)
	tracker, _, err := manager.attach(
		ViewerSessionAttachRequest{},
		ViewerSessionIdentity{
			OwnerID: "viewer-a",
			ScopeID: "boat-a",
		},
		socketTrackerConfig{
			sr:     registry,
			limits: SocketHandlerLimits{}.withDefaults(),
		},
	)
	if err != nil {
		t.Fatalf("manager.attach failed: %v", err)
	}
	if err := tracker.reconcileSessionManifest(ViewerSessionAttachRequest{
		StoreSubscriptions: []ViewerSessionStoreSubscription{{
			StoreName: viewerSocketTestStoreName,
		}},
	}, AccessLevelPublic); err != nil {
		t.Fatalf("initial manifest failed: %v", err)
	}

	// Simulate the initial baseline having been flushed to an attached client.
	tracker.sessionBufferMutex.Lock()
	tracker.sessionStoreUpdates = map[string]*bufferedViewerStoreUpdate{}
	tracker.sessionStoreUpdateBytes = 0
	tracker.sessionBufferMutex.Unlock()

	applyViewerSessionTestPartial(t, registry, &viewerSocketTestPartial{
		Values: NewPartialMap[string, int]().Set("a", 1),
	})
	applyViewerSessionTestPartial(t, registry, &viewerSocketTestPartial{
		Values: NewPartialMap[string, int]().Set("b", 2),
	})

	tracker.sessionBufferMutex.Lock()
	update := tracker.sessionStoreUpdates[viewerSocketTestStoreName]
	if update == nil || update.partial == nil || update.fullState != nil {
		tracker.sessionBufferMutex.Unlock()
		t.Fatalf("two partials did not retain one partial accumulator: %#v", update)
	}
	partial := update.partial
	tracker.sessionBufferMutex.Unlock()
	partialState := &viewerSocketTestState{Values: map[string]int{}}
	partial.ApplyTo(partialState)
	assertMapEqual(t, map[string]int{"a": 1, "b": 2}, partialState.Values)

	fullState := &viewerSocketTestState{
		Values: map[string]int{"full": 3},
		Other:  4,
	}
	fullBytes, err := SerializeToBytes(fullState, nil)
	if err != nil {
		t.Fatalf("serialize full state failed: %v", err)
	}
	if err := registry.SetFullStateToStore(viewerSocketTestStoreName, fullBytes); err != nil {
		t.Fatalf("SetFullStateToStore failed: %v", err)
	}
	applyViewerSessionTestPartial(t, registry, &viewerSocketTestPartial{
		Other: Ptr(9),
	})

	tracker.sessionBufferMutex.Lock()
	update = tracker.sessionStoreUpdates[viewerSocketTestStoreName]
	if update == nil || update.fullState == nil || update.partial != nil {
		tracker.sessionBufferMutex.Unlock()
		t.Fatalf("full state did not supersede the partial accumulator: %#v", update)
	}
	retainedFull, ok := update.fullState.(*viewerSocketTestState)
	tracker.sessionBufferMutex.Unlock()
	if !ok {
		t.Fatalf("retained full state has type %T", update.fullState)
	}
	assertMapEqual(t, map[string]int{"full": 3}, retainedFull.Values)
	if retainedFull.Other != 9 {
		t.Fatalf("partial after full state was not applied: Other=%d", retainedFull.Other)
	}

	replacement := &viewerSocketTestState{
		Values: map[string]int{"replacement": 10},
		Other:  11,
	}
	replacementBytes, err := SerializeToBytes(replacement, nil)
	if err != nil {
		t.Fatalf("serialize replacement failed: %v", err)
	}
	if err := registry.SetFullStateToStore(viewerSocketTestStoreName, replacementBytes); err != nil {
		t.Fatalf("replace full state failed: %v", err)
	}
	tracker.sessionBufferMutex.Lock()
	retainedFull = tracker.sessionStoreUpdates[viewerSocketTestStoreName].
		fullState.(*viewerSocketTestState)
	tracker.sessionBufferMutex.Unlock()
	assertMapEqual(t, replacement.Values, retainedFull.Values)
	if retainedFull.Other != replacement.Other {
		t.Fatalf("second full state did not replace the first: %#v", retainedFull)
	}
}

func TestViewerSessionManifestChangeRebuildsRetainedKeyBaseline(t *testing.T) {
	store := NewRelayStore[
		viewerSocketTestState,
		*viewerSocketTestState,
		*viewerSocketTestPartial,
	](viewerSocketTestStoreName, &viewerSocketTestState{
		Values: map[string]int{
			"a": 1,
			"b": 2,
		},
	}, AccessLevelPublic)
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	manager := NewViewerSessionManager(ViewerSessionManagerOptions{})
	t.Cleanup(manager.Close)
	tracker, _, err := manager.attach(
		ViewerSessionAttachRequest{},
		ViewerSessionIdentity{OwnerID: "viewer-a", ScopeID: "boat-a"},
		socketTrackerConfig{
			sr:     registry,
			limits: SocketHandlerLimits{}.withDefaults(),
		},
	)
	if err != nil {
		t.Fatalf("manager.attach failed: %v", err)
	}
	initial := ViewerSessionAttachRequest{
		StoreSubscriptions: []ViewerSessionStoreSubscription{
			{StoreName: viewerSocketTestStoreName, Key: "values%&a"},
			{StoreName: viewerSocketTestStoreName, Key: "values%&b"},
		},
	}
	if err := tracker.reconcileSessionManifest(initial, AccessLevelPublic); err != nil {
		t.Fatalf("initial manifest failed: %v", err)
	}
	tracker.sessionBufferMutex.Lock()
	tracker.sessionStoreUpdates = map[string]*bufferedViewerStoreUpdate{}
	tracker.sessionStoreUpdateBytes = 0
	tracker.sessionBufferMutex.Unlock()

	applyViewerSessionTestPartial(t, registry, &viewerSocketTestPartial{
		Values: NewPartialMap[string, int]().
			Set("a", 10).
			Set("b", 20),
	})
	staleGeneration := tracker.currentStoreSubscriptionGeneration(viewerSocketTestStoreName)
	if err := tracker.reconcileSessionManifest(ViewerSessionAttachRequest{
		StoreSubscriptions: []ViewerSessionStoreSubscription{{
			StoreName: viewerSocketTestStoreName,
			Key:       "values%&b",
		}},
	}, AccessLevelPublic); err != nil {
		t.Fatalf("resumed manifest failed: %v", err)
	}
	staleMessage, err := buildPartialStoreUpdateMessage(
		viewerSocketTestStoreName,
		&viewerSocketTestPartial{
			Values: NewPartialMap[string, int]().Set("a", 999),
		},
	)
	if err != nil {
		t.Fatalf("build stale partial failed: %v", err)
	}
	staleMessage.StoreGeneration = staleGeneration
	if err := tracker.bufferDetachedMessage(staleMessage); err != nil {
		t.Fatalf("buffer stale partial failed: %v", err)
	}

	tracker.sessionBufferMutex.Lock()
	update := tracker.sessionStoreUpdates[viewerSocketTestStoreName]
	if update == nil || update.partial == nil {
		tracker.sessionBufferMutex.Unlock()
		t.Fatalf("changed manifest did not retain a keyed baseline: %#v", update)
	}
	partial := update.partial
	tracker.sessionBufferMutex.Unlock()
	state := &viewerSocketTestState{Values: map[string]int{}}
	partial.ApplyTo(state)
	assertMapEqual(t, map[string]int{"b": 20}, state.Values)
}

func TestViewerSessionBufferedStoreUpdatesRetainSubscriptionGeneration(t *testing.T) {
	const generation = 7
	tests := []struct {
		name   string
		update *bufferedViewerStoreUpdate
	}{
		{
			name: "full state",
			update: &bufferedViewerStoreUpdate{
				generation: generation,
				fullState: &viewerSocketTestState{
					Values: map[string]int{"a": 1},
				},
			},
		},
		{
			name: "partial",
			update: &bufferedViewerStoreUpdate{
				generation: generation,
				partial: &viewerSocketTestPartial{
					Values: NewPartialMap[string, int]().Set("a", 1),
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err := buildBufferedStoreUpdateMessage(
				viewerSocketTestStoreName,
				test.update,
			)
			if err != nil {
				t.Fatalf("build buffered store update failed: %v", err)
			}
			if message.StoreName != viewerSocketTestStoreName {
				t.Fatalf("store name = %q, want %q", message.StoreName, viewerSocketTestStoreName)
			}
			if message.StoreGeneration != generation {
				t.Fatalf("store generation = %d, want %d", message.StoreGeneration, generation)
			}
		})
	}
}

func applyViewerSessionTestPartial(
	t *testing.T,
	registry *StoreRegistry,
	partial *viewerSocketTestPartial,
) {
	t.Helper()
	partialBytes, err := SerializeToBytes(partial, nil)
	if err != nil {
		t.Fatalf("serialize partial failed: %v", err)
	}
	if err := registry.ApplyPartialToStore(viewerSocketTestStoreName, partialBytes); err != nil {
		t.Fatalf("ApplyPartialToStore failed: %v", err)
	}
}
