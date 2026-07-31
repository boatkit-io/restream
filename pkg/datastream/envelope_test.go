package datastream

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestEnvelopeRoundTripsFrameAndBlockSet(t *testing.T) {
	tests := []Envelope{
		{
			StreamID:          "CameraMedia/Video/camera-a",
			Generation:        2,
			Sequence:          11,
			TimestampUnixNano: 42,
			PayloadType:       PayloadFrame,
			FrameID:           7,
			Flags:             FlagRecovery | FlagDiscontinuity,
			Format:            "video/h264; framing=cmaf",
			Payload:           []byte{1, 2, 3},
		},
		{
			StreamID:          "Radar/Spokes/radar-a",
			Generation:        4,
			Sequence:          12,
			TimestampUnixNano: 43,
			PayloadType:       PayloadBlockSet,
			FrameID:           8,
			Format:            "application/x-radar-spokes-v1",
			FirstIndex:        16,
			ItemCount:         8,
			TotalItemCount:    2048,
			Payload:           []byte{4, 5, 6},
		},
		{
			StreamID:          "Radar/Spokes/radar-a",
			Generation:        4,
			Sequence:          13,
			TimestampUnixNano: 44,
			PayloadType:       PayloadBlockSet,
			FrameID:           8,
			Flags:             FlagCommit,
			Format:            "application/x-radar-spokes-v1",
			TotalItemCount:    2048,
		},
	}

	for _, envelope := range tests {
		encoded, err := Encode(envelope)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		decoded, err := Decode(encoded, 1024)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		if !reflect.DeepEqual(decoded, envelope) {
			t.Fatalf("decoded envelope = %#v, want %#v", decoded, envelope)
		}
	}
}

func TestEnvelopeRejectsPartialRecoveryAndIncompleteCommit(t *testing.T) {
	block := Envelope{
		StreamID:       "Radar/Spokes/radar-a",
		Generation:     1,
		Sequence:       1,
		PayloadType:    PayloadBlockSet,
		FrameID:        1,
		Format:         "radar-v1",
		FirstIndex:     0,
		ItemCount:      1,
		TotalItemCount: 2,
		Payload:        []byte{1},
	}
	block.Flags = FlagRecovery
	if err := block.Validate(1024); err == nil {
		t.Fatal("Validate accepted a BlockSet recovery payload")
	}

	block.Flags = FlagCommit
	if err := block.Validate(1024); err == nil {
		t.Fatal("Validate accepted a commit carrying a partial range")
	}
}

func TestDecodeRejectsTrailingOrOversizedPayload(t *testing.T) {
	envelope := Envelope{
		StreamID:    "Sonar/Ping/sonar-a",
		Generation:  1,
		Sequence:    1,
		PayloadType: PayloadFrame,
		FrameID:     1,
		Flags:       FlagRecovery,
		Format:      "sonar-v1",
		Payload:     bytes.Repeat([]byte{1}, 16),
	}
	encoded, err := Encode(envelope)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if _, err := Decode(append(encoded, 0), 1024); err == nil {
		t.Fatal("Decode accepted trailing bytes")
	}
	if _, err := Decode(encoded, 8); err == nil {
		t.Fatal("Decode accepted a payload over the caller limit")
	}
}

func TestSchedulerFallsBackToNewestRecoveryFrame(t *testing.T) {
	now := time.Unix(100, 0)
	scheduler := NewScheduler(SchedulerConfig{
		MaxPendingBytes: 1024 * 1024,
		MaxPendingUnits: 2,
		Now:             func() time.Time { return now },
	})
	if err := scheduler.Register("Radar/Spokes/radar-a", StreamPolicy{}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		if err := scheduler.Submit(testBlockEnvelope(sequence)); err != nil {
			t.Fatalf("Submit block %d failed: %v", sequence, err)
		}
	}
	if err := scheduler.Submit(testBlockEnvelope(3)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("third block error = %v, want ErrQueueFull", err)
	}
	if !scheduler.NeedsRecovery("Radar/Spokes/radar-a") {
		t.Fatal("scheduler did not request recovery after dropping a BlockSet unit")
	}

	recovery := Envelope{
		StreamID:    "Radar/Spokes/radar-a",
		Generation:  1,
		Sequence:    4,
		PayloadType: PayloadFrame,
		FrameID:     2,
		Flags:       FlagRecovery | FlagDiscontinuity,
		Format:      "radar-sweep-v1",
		Payload:     []byte{9},
	}
	if err := scheduler.Submit(recovery); err != nil {
		t.Fatalf("Submit recovery failed: %v", err)
	}
	if scheduler.NeedsRecovery("Radar/Spokes/radar-a") {
		t.Fatal("accepted recovery frame left recovery requested")
	}
	pendingUnits, _, droppedUnits := scheduler.Stats()
	if pendingUnits != 1 || droppedUnits != 2 {
		t.Fatalf("scheduler stats = pending %d dropped %d, want pending 1 dropped 2", pendingUnits, droppedUnits)
	}
	got, ok := scheduler.Next()
	if !ok || !reflect.DeepEqual(got, recovery) {
		t.Fatalf("Next = %#v, %t; want recovery frame", got, ok)
	}
}

func TestSchedulerRejectsDependentWorkUntilDiscontinuousRecovery(t *testing.T) {
	scheduler := NewScheduler(SchedulerConfig{
		MaxPendingBytes: 1024 * 1024,
		MaxPendingUnits: 1,
	})
	const streamID = "Radar/Spokes/radar-a"
	if err := scheduler.Register(streamID, StreamPolicy{}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if err := scheduler.Submit(testBlockEnvelope(1)); err != nil {
		t.Fatalf("first block failed: %v", err)
	}
	if err := scheduler.Submit(testBlockEnvelope(2)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overflow error = %v, want ErrQueueFull", err)
	}

	if err := scheduler.Submit(testBlockEnvelope(3)); !errors.Is(err, ErrNeedsRecovery) {
		t.Fatalf("dependent block error = %v, want ErrNeedsRecovery", err)
	}
	recoveryWithoutDiscontinuity := testFrameEnvelope(streamID, 4)
	recoveryWithoutDiscontinuity.Flags = FlagRecovery
	if err := scheduler.Submit(recoveryWithoutDiscontinuity); !errors.Is(err, ErrNeedsRecovery) {
		t.Fatalf("continuous recovery error = %v, want ErrNeedsRecovery", err)
	}

	recovery := testFrameEnvelope(streamID, 5)
	recovery.Flags = FlagRecovery | FlagDiscontinuity
	if err := scheduler.Submit(recovery); err != nil {
		t.Fatalf("discontinuous recovery failed: %v", err)
	}
	if scheduler.NeedsRecovery(streamID) {
		t.Fatal("scheduler still requires recovery after accepting a recovery boundary")
	}
	got, ok := scheduler.Next()
	if !ok || !reflect.DeepEqual(got, recovery) {
		t.Fatalf("Next = %#v, %t; want the recovery frame", got, ok)
	}
}

func TestSchedulerPromotesAStreamAtItsSilenceDeadline(t *testing.T) {
	now := time.Unix(100, 0)
	scheduler := NewScheduler(SchedulerConfig{
		MaxPendingBytes: 1024 * 1024,
		MaxPendingUnits: 10,
		Now:             func() time.Time { return now },
	})
	if err := scheduler.Register("camera", StreamPolicy{Weight: 10}); err != nil {
		t.Fatalf("Register camera failed: %v", err)
	}
	if err := scheduler.Register("sonar", StreamPolicy{Weight: 1, MaxSilence: time.Second}); err != nil {
		t.Fatalf("Register sonar failed: %v", err)
	}
	if err := scheduler.Submit(testFrameEnvelope("camera", 1)); err != nil {
		t.Fatalf("Submit camera failed: %v", err)
	}
	if err := scheduler.Submit(testFrameEnvelope("sonar", 1)); err != nil {
		t.Fatalf("Submit sonar failed: %v", err)
	}
	now = now.Add(2 * time.Second)
	got, ok := scheduler.Next()
	if !ok || got.StreamID != "sonar" {
		t.Fatalf("Next stream = %q, %t; want overdue sonar", got.StreamID, ok)
	}
}

func testBlockEnvelope(sequence uint64) Envelope {
	return Envelope{
		StreamID:       "Radar/Spokes/radar-a",
		Generation:     1,
		Sequence:       sequence,
		PayloadType:    PayloadBlockSet,
		FrameID:        1,
		Format:         "radar-spokes-v1",
		FirstIndex:     uint32(sequence - 1),
		ItemCount:      1,
		TotalItemCount: 4,
		Payload:        []byte{byte(sequence)},
	}
}

func testFrameEnvelope(streamID string, sequence uint64) Envelope {
	return Envelope{
		StreamID:    streamID,
		Generation:  1,
		Sequence:    sequence,
		PayloadType: PayloadFrame,
		FrameID:     sequence,
		Format:      "test-v1",
		Payload:     []byte{byte(sequence)},
	}
}
