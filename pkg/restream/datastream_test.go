package restream

import (
	"context"
	"strings"
	"testing"
)

func TestDataStreamDispatcherAuthorizesRegistrationIndependentlyFromStore(t *testing.T) {
	dispatcher := NewDataStreamDispatcher()
	var handledKey string
	var handledSubscribe bool
	dispatcher.RegisterDataStream(
		"CameraStore",
		"Video",
		AccessLevel(3),
		func(_ context.Context, key string, subscribe bool) error {
			handledKey = key
			handledSubscribe = subscribe
			return nil
		},
	)
	subscription := DataStreamSubscription{
		StoreName:  "CameraStore",
		StreamName: "Video",
		Key:        "camera-a",
	}

	if err := dispatcher.Dispatch(
		context.Background(),
		subscription,
		AccessLevel(2),
		true,
	); err == nil || !strings.Contains(err.Error(), "insufficient access") {
		t.Fatalf("Dispatch below stream access = %v, want access error", err)
	}
	if handledKey != "" {
		t.Fatal("unauthorized stream dispatch reached the handler")
	}
	if err := dispatcher.Dispatch(
		context.Background(),
		subscription,
		AccessLevel(3),
		true,
	); err != nil {
		t.Fatalf("authorized Dispatch failed: %v", err)
	}
	if handledKey != "camera-a" || !handledSubscribe {
		t.Fatalf("handler received key=%q subscribe=%t", handledKey, handledSubscribe)
	}
}

func TestDataStreamDispatcherRegistrationIncludesStoreName(t *testing.T) {
	dispatcher := NewDataStreamDispatcher()
	dispatcher.RegisterDataStream(
		"CameraStore",
		"Video",
		AccessLevelPublic,
		func(context.Context, string, bool) error { return nil },
	)
	dispatcher.RegisterDataStream(
		"SonarStore",
		"Video",
		AccessLevelPublic,
		func(context.Context, string, bool) error { return nil },
	)

	err := dispatcher.CheckAccess(DataStreamSubscription{
		StoreName:  "RadarStore",
		StreamName: "Video",
		Key:        "radar-a",
	}, AccessLevelPublic)
	if err == nil {
		t.Fatal("CheckAccess accepted an unregistered store/stream pair")
	}
}
