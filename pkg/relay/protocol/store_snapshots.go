package protocol

import (
	"fmt"
	"unicode/utf8"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

// StoreSnapshotRequestPacket requests a fresh baseline of an active on-demand
// store. Send only to a device advertising StoreSnapshots. RequestID is an opaque
// correlation value unique across relay owner wakes within the connection.
// The response carries the matching request ID and full state, followed by partials.
type StoreSnapshotRequestPacket struct {
	StoreName string
	RequestID string
}

func (*StoreSnapshotRequestPacket) Kind() PacketKind { return KindStoreSnapshotRequest }

// Validate bounds control metadata independently of the serialized store size.
func (p *StoreSnapshotRequestPacket) Validate() error {
	return validateStoreSnapshot(p.StoreName, p.RequestID)
}

// StoreSnapshotResponsePacket carries a detached full state answering RequestID.
// It is used for a cache miss on a healthy relay connection. A first subscription
// still receives an ordinary FullState packet. Relay owner recreation closes both
// transports and uses normal connection setup, never a subscription repair flow.
type StoreSnapshotResponsePacket struct {
	StoreName string
	RequestID string
	Data      []byte
}

func (*StoreSnapshotResponsePacket) Kind() PacketKind { return KindStoreSnapshotResponse }

func validateStoreSnapshot(storeName, requestID string) error {
	if len(storeName) == 0 || len(storeName) > 256 || !utf8.ValidString(storeName) ||
		len(requestID) == 0 || len(requestID) > 128 || !utf8.ValidString(requestID) {
		return fmt.Errorf("invalid store snapshot metadata")
	}
	return nil
}

func encodeStoreSnapshot(w *binarystreams.Writer, storeName, requestID string) error {
	if err := validateStoreSnapshot(storeName, requestID); err != nil {
		return err
	}
	if err := writeString(w, storeName); err != nil {
		return err
	}
	return writeString(w, requestID)
}

func decodeStoreSnapshot(r *binarystreams.Reader) (string, string, error) {
	storeName, err := readString(r)
	if err != nil {
		return "", "", err
	}
	requestID, err := readString(r)
	if err != nil {
		return "", "", err
	}
	return storeName, requestID, validateStoreSnapshot(storeName, requestID)
}
