package ffrpc

import (
	"fmt"

	"github.com/boatkit-io/restream/pkg/restream"
)

func ExampleRegisterTransmitAudioFFRPC() {
	rpcd := restream.NewRPCDispatcher(nil)
	radio := &Radio{
		OnAudio: func(radioID string, sequence uint32, audio []byte) error {
			fmt.Printf("%s #%d: %v\n", radioID, sequence, audio)
			return nil
		},
	}
	RegisterTransmitAudioFFRPC(rpcd, radio)

	requestBytes, err := restream.SerializeToBytes(
		&RadioTransmitAudioRequest{
			RadioID:  "radio-a",
			Sequence: 12,
			Audio:    []byte{1, 2, 3},
		},
		nil,
	)
	if err != nil {
		panic(err)
	}
	handled, err := rpcd.FireFFRPC(
		"Radio.TransmitAudio",
		AccessLevelAdmin,
		requestBytes,
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("handled: %t\n", handled)

	// Output:
	// radio-a #12: [1 2 3]
	// handled: true
}
