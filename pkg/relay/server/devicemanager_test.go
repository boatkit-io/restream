package server

import (
	"testing"

	"github.com/boatkit-io/restream/pkg/restream"
)

func TestDeviceManagerCreatesConfiguredStores(t *testing.T) {
	configured := 0
	manager := NewDeviceManager(DeviceManagerConfig{
		Stores: func(deviceID string) ([]restream.Store, error) {
			if deviceID != "device-1" {
				t.Fatalf("deviceID = %q, want device-1", deviceID)
			}
			return []restream.Store{
				restream.NewRelayStore[testState, *testState, *testPartial]("TestStore", &testState{}, restream.AccessLevelPublic),
			}, nil
		},
		ConfigureDevice: func(device *Device) error {
			configured++
			if device.DeviceID != "device-1" {
				t.Fatalf("configured deviceID = %q, want device-1", device.DeviceID)
			}
			return nil
		},
	})

	device, err := manager.GetDevice("device-1")
	if err != nil {
		t.Fatalf("GetDevice failed: %v", err)
	}
	if !manager.HasDevice("device-1") {
		t.Fatal("HasDevice = false, want true")
	}
	if !device.StoreRegistry.IsStoreValid("TestStore") {
		t.Fatal("TestStore was not registered")
	}
	if configured != 1 {
		t.Fatalf("configured = %d, want 1", configured)
	}

	sameDevice, err := manager.GetDevice("device-1")
	if err != nil {
		t.Fatalf("second GetDevice failed: %v", err)
	}
	if sameDevice != device {
		t.Fatal("GetDevice returned a different device for the same id")
	}
	if configured != 1 {
		t.Fatalf("configured after second GetDevice = %d, want 1", configured)
	}
}
