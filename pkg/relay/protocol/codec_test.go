package protocol

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestDeviceHelloRoundTrip(t *testing.T) {
	hello := &DeviceHello{
		ProtocolVersion: CurrentVersion,
		DeviceID:        "device-alpha",
		AuthType:        "shared-key",
		AuthData:        []byte("opaque-auth-data"),
		Metadata: map[string]string{
			"app":    "tictactoe",
			"schema": "v1",
		},
	}

	encoded, err := EncodeDeviceHello(hello)
	if err != nil {
		t.Fatalf("EncodeDeviceHello failed: %v", err)
	}

	decoded, err := DecodeDeviceHello(encoded)
	if err != nil {
		t.Fatalf("DecodeDeviceHello failed: %v", err)
	}
	if !reflect.DeepEqual(decoded, hello) {
		t.Fatalf("decoded hello = %#v, want %#v", decoded, hello)
	}

	encodedAgain, err := EncodeDeviceHello(decoded)
	if err != nil {
		t.Fatalf("EncodeDeviceHello decoded failed: %v", err)
	}
	if !bytes.Equal(encodedAgain, encoded) {
		t.Fatal("device hello encoding is not deterministic")
	}
}

func TestPacketRoundTrips(t *testing.T) {
	tests := []struct {
		name string
		in   Packet
		kind PacketKind
	}{
		{
			name: "connected",
			in: &ConnectedPacket{
				ProtocolVersion: CurrentVersion,
				Metadata:        map[string]string{"relay": "test"},
			},
			kind: KindConnected,
		},
		{
			name: "full state",
			in:   NewFullStatePacket("BoardStore", []byte{1, 2, 3}),
			kind: KindFullState,
		},
		{
			name: "partial state",
			in:   NewPartialStatePacket("BoardStore", []byte{4, 5, 6}),
			kind: KindPartialState,
		},
		{
			name: "event",
			in:   &EventPacket{EventName: "ServerTime", Data: []byte{7, 8}},
			kind: KindEvent,
		},
		{
			name: "rpc call",
			in: &RPCCallPacket{
				RPCID:       42,
				MethodName:  "BoardStore.PlaceToken",
				AccessLevel: 3,
				Request:     []byte{9, 10, 11},
			},
			kind: KindRPCCall,
		},
		{
			name: "rpc response",
			in:   &RPCResponsePacket{RPCID: 42, Response: []byte{12, 13}},
			kind: KindRPCResponse,
		},
		{
			name: "store subscription",
			in: &StoreSubscriptionPacket{
				StoreName: "TimeSeriesHistory",
				Key:       "samples%&Water_Depth_Auto",
				Action:    StoreSubscribe,
			},
			kind: KindStoreSubscription,
		},
		{
			name: "named custom",
			in:   &CustomPacket{Name: "com.example.metrics", Payload: []byte{14, 15, 16}},
			kind: KindCustom,
		},
		{
			name: "fixed application raw",
			in:   &RawPacket{PacketKind: FirstApplicationPacketKind, Payload: []byte{17, 18}},
			kind: FirstApplicationPacketKind,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodePacket(tt.in)
			if err != nil {
				t.Fatalf("EncodePacket failed: %v", err)
			}
			if gotKind := PacketKind(encoded[0]); gotKind != tt.kind {
				t.Fatalf("encoded kind = %d, want %d", gotKind, tt.kind)
			}

			decoded, err := DecodePacket(encoded)
			if err != nil {
				t.Fatalf("DecodePacket failed: %v", err)
			}
			if !reflect.DeepEqual(decoded, tt.in) {
				t.Fatalf("decoded packet = %#v, want %#v", decoded, tt.in)
			}
		})
	}
}

func TestUnknownPacketDecodesAsRaw(t *testing.T) {
	encoded := []byte{42, 1, 2, 3}

	decoded, err := DecodePacket(encoded)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}

	raw, ok := decoded.(*RawPacket)
	if !ok {
		t.Fatalf("decoded packet type = %T, want *RawPacket", decoded)
	}
	if raw.PacketKind != PacketKind(42) {
		t.Fatalf("raw kind = %d, want 42", raw.PacketKind)
	}
	if !bytes.Equal(raw.Payload, []byte{1, 2, 3}) {
		t.Fatalf("raw payload = %v, want [1 2 3]", raw.Payload)
	}
}

func TestZeroPacketKindIsRejected(t *testing.T) {
	if _, err := DecodePacket([]byte{0}); err == nil {
		t.Fatal("DecodePacket accepted packet kind zero")
	}
	if _, err := EncodePacket(&RawPacket{PacketKind: 42}); err == nil {
		t.Fatal("EncodePacket accepted a raw packet outside the application-defined range")
	}
}

func TestKnownPacketRejectsTrailingBytes(t *testing.T) {
	encoded, err := EncodePacket(&EventPacket{EventName: "ServerTime", Data: []byte{1}})
	if err != nil {
		t.Fatalf("EncodePacket failed: %v", err)
	}

	_, err = DecodePacket(append(encoded, 99))
	if err == nil {
		t.Fatal("DecodePacket succeeded with trailing bytes, want error")
	}
}

func TestCustomPacketRequiresName(t *testing.T) {
	if _, err := EncodePacket(&CustomPacket{Payload: []byte{1}}); err == nil {
		t.Fatal("EncodePacket accepted an unnamed custom packet")
	}

	encodedUnnamedCustom := []byte{byte(KindCustom), 0, 0, 0, 0, 0, 0}
	if _, err := DecodePacket(encodedUnnamedCustom); err == nil {
		t.Fatal("DecodePacket accepted an unnamed custom packet")
	}
}

func TestStoreSubscriptionRejectsInvalidAction(t *testing.T) {
	if _, err := EncodePacket(&StoreSubscriptionPacket{
		StoreName: "TestStore",
		Key:       "values%&a",
		Action:    StoreSubscriptionAction(99),
	}); err == nil {
		t.Fatal("EncodePacket accepted an invalid store subscription action")
	}

	encoded, err := EncodePacket(&StoreSubscriptionPacket{
		StoreName: "TestStore",
		Key:       "values%&a",
		Action:    StoreSubscribe,
	})
	if err != nil {
		t.Fatalf("EncodePacket valid store subscription failed: %v", err)
	}
	encoded[len(encoded)-1] = 99
	if _, err := DecodePacket(encoded); err == nil {
		t.Fatal("DecodePacket accepted an invalid store subscription action")
	}
}

func TestOversizedStringsAreRejected(t *testing.T) {
	oversizedName := strings.Repeat("a", maxUint16+1)

	if _, err := EncodePacket(NewFullStatePacket(oversizedName, nil)); err == nil {
		t.Fatal("EncodePacket accepted an oversized store name")
	}
	if _, err := EncodeDeviceHello(&DeviceHello{
		ProtocolVersion: CurrentVersion,
		DeviceID:        oversizedName,
	}); err == nil {
		t.Fatal("EncodeDeviceHello accepted an oversized device ID")
	}
}

func TestPacketKindHelpers(t *testing.T) {
	if !IsStandardKind(KindFullState) {
		t.Fatal("KindFullState should be standard")
	}
	if IsStandardKind(FirstApplicationPacketKind) {
		t.Fatal("FirstApplicationPacketKind should not be standard")
	}
	if !IsApplicationKind(FirstApplicationPacketKind) {
		t.Fatal("FirstApplicationPacketKind should be application-defined")
	}
	if IsApplicationKind(KindCustom) {
		t.Fatal("KindCustom should use named custom packet handling, not fixed application handling")
	}
}
