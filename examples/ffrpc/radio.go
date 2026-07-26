// Package ffrpc demonstrates request-only, fire-and-forget RPC calls.
package ffrpc

import (
	"reflect"

	"github.com/boatkit-io/restream/pkg/restream"
)

const AccessLevelAdmin restream.AccessLevel = 2

// Radio receives already encoded microphone frames from a remote client.
type Radio struct {
	OnAudio func(radioID string, sequence uint32, audio []byte) error
}

// RegisterTransmitAudioFFRPC registers a request-only audio handler.
func RegisterTransmitAudioFFRPC(rpcd *restream.RPCDispatcher, radio *Radio) {
	rpcd.RegisterFFRPCHandler("Radio.TransmitAudio", AccessLevelAdmin,
		func(radioID string, sequence uint32, audio []byte) error {
			return radio.OnAudio(radioID, sequence, audio)
		}, reflect.TypeFor[RadioTransmitAudioRequest]())
}
