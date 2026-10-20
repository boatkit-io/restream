package protocol

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
)

func TestStoreSnapshotStandardWirePackets(t *testing.T) {
	tests := []struct {
		packet Packet
		hex    string
	}{
		{&StoreSnapshotRequestPacket{StoreName: "S", RequestID: "w:1"}, "100100530300773a31"},
		{&StoreSnapshotResponsePacket{StoreName: "S", RequestID: "w:1", Data: []byte{42}}, "110100530300773a31010000002a"},
	}
	for _, tt := range tests {
		encoded, err := EncodePacket(tt.packet)
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(encoded) != tt.hex {
			t.Fatalf("wire = %x, want %s", encoded, tt.hex)
		}
		decoded, err := DecodePacket(encoded)
		if err != nil || !reflect.DeepEqual(decoded, tt.packet) {
			t.Fatalf("round trip = %#v, %v", decoded, err)
		}
		for length := 0; length < len(encoded); length++ {
			if _, err := DecodePacket(encoded[:length]); err == nil {
				t.Fatalf("accepted truncated kind %d at %d", tt.packet.Kind(), length)
			}
		}
		if _, err := DecodePacket(append(encoded, 0)); err == nil {
			t.Fatal("accepted trailing control bytes")
		}
	}
}

func TestStoreSnapshotMetadataBoundsOnEncodeAndDecode(t *testing.T) {
	for _, fields := range [][2]string{
		{"", "i"}, {"S", ""}, {strings.Repeat("s", 257), "i"},
		{"S", strings.Repeat("i", 129)}, {"\xff", "i"}, {"S", "\xff"},
		{strings.Repeat("☃", 86), "i"}, {"S", strings.Repeat("☃", 43)},
	} {
		for _, kind := range []PacketKind{KindStoreSnapshotRequest, KindStoreSnapshotResponse} {
			var packet Packet = &StoreSnapshotRequestPacket{StoreName: fields[0], RequestID: fields[1]}
			if kind == KindStoreSnapshotResponse {
				packet = &StoreSnapshotResponsePacket{StoreName: fields[0], RequestID: fields[1]}
			}
			if _, err := EncodePacket(packet); err == nil {
				t.Fatalf("accepted invalid metadata %#v", fields)
			}
			// Assemble unchecked strings to prove decoder validation independently.
			w, buf := newMemoryWriter()
			if err := w.WriteByte(byte(kind)); err != nil {
				t.Fatal(err)
			}
			for _, value := range fields {
				if err := writeString(w, value); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if _, err := DecodePacket(buf.Bytes()); err == nil {
				t.Fatalf("decoded invalid metadata %#v", fields)
			}
		}
	}
	valid := &StoreSnapshotRequestPacket{StoreName: strings.Repeat("s", 256), RequestID: strings.Repeat("i", 128)}
	if _, err := EncodePacket(valid); err != nil {
		t.Fatal(err)
	}
}

func TestStoreSnapshotCapabilityIsTypedReservedAndOptional(t *testing.T) {
	application := map[string]string{"version": "synthetic", DeviceStoreSnapshotsCapabilityMetadataKey: "1"}
	without := DeviceMetadataWithCapabilities(application, DeviceCapabilities{})
	if CapabilitiesFromDeviceMetadata(without).StoreSnapshots {
		t.Fatal("application metadata spoofed capability")
	}
	if application[DeviceStoreSnapshotsCapabilityMetadataKey] != "1" {
		t.Fatal("mutated caller metadata")
	}
	metadata := DeviceMetadataWithCapabilities(application, DeviceCapabilities{StoreSnapshots: true})
	encoded, err := EncodeDeviceHello(&DeviceHello{ProtocolVersion: CurrentVersion, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	hello, err := DecodeDeviceHello(encoded)
	if err != nil || !CapabilitiesFromDeviceMetadata(hello.Metadata).StoreSnapshots {
		t.Fatal("snapshot capability did not round trip")
	}
	if CurrentVersion != 9 || CapabilitiesFromDeviceMetadata(nil).StoreSnapshots {
		t.Fatal("base or legacy negotiation changed")
	}
	// Older v9 framing remains byte-for-byte unchanged for ordinary full states.
	state, err := EncodePacket(NewFullStatePacket("S", []byte{42}))
	if err != nil || !bytes.Equal(state, []byte{2, 1, 0, 'S', 1, 0, 0, 0, 42}) {
		t.Fatalf("full state wire changed: %x, %v", state, err)
	}
}
