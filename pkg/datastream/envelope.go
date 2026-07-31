// Package datastream defines Restream's transport-independent high-bandwidth
// data plane. The ordinary Restream websocket remains the authenticated control
// plane; these envelopes travel over a separate bounded transport.
package datastream

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
)

const (
	wireMagic         = "RSD1"
	fixedHeaderSize   = 60
	maxStreamIDBytes  = 4 * 1024
	maxFormatBytes    = 4 * 1024
	defaultMaxPayload = 32 * 1024 * 1024
	allEnvelopeFlags  = FlagRecovery | FlagDiscontinuity | FlagCommit
)

// PayloadType identifies the recovery semantics of an envelope.
type PayloadType uint8

const (
	// PayloadFrame is one atomically delivered whole frame. Frames may depend on
	// earlier frames; FlagRecovery marks the independently consumable recovery
	// points. Examples include an encoded video access unit, a sonar ping, an
	// audio block, or a complete radar sweep.
	PayloadFrame PayloadType = iota + 1
	// PayloadBlockSet carries indexed portions of one logical array frame. A
	// zero-length commit envelope follows only after every item is available.
	PayloadBlockSet
)

// Flags describe delivery and recovery boundaries.
type Flags uint8

const (
	// FlagRecovery marks a frame suitable for a new or recovering consumer.
	FlagRecovery Flags = 1 << iota
	// FlagDiscontinuity tells consumers to discard incomplete prior state.
	FlagDiscontinuity
	// FlagCommit atomically publishes a previously delivered BlockSet frame.
	FlagCommit
)

// Envelope is one independently scheduled data-plane unit.
type Envelope struct {
	StreamID          string
	Generation        uint64
	Sequence          uint64
	TimestampUnixNano int64
	PayloadType       PayloadType
	FrameID           uint64
	Flags             Flags
	Format            string

	// BlockSet indexing. These must be zero for Frame payloads. Commit
	// envelopes have TotalItemCount set and no range or payload.
	FirstIndex     uint32
	ItemCount      uint32
	TotalItemCount uint32

	Payload []byte
}

// Validate enforces the atomic recovery and BlockSet commit invariants.
func (e Envelope) Validate(maxPayloadBytes int) error {
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = defaultMaxPayload
	}
	switch {
	case strings.TrimSpace(e.StreamID) == "":
		return fmt.Errorf("data stream ID is required")
	case len(e.StreamID) > maxStreamIDBytes:
		return fmt.Errorf("data stream ID is too long")
	case e.Generation == 0:
		return fmt.Errorf("data stream generation must be non-zero")
	case e.Sequence == 0:
		return fmt.Errorf("data stream sequence must be non-zero")
	case e.FrameID == 0:
		return fmt.Errorf("data stream frame ID must be non-zero")
	case strings.TrimSpace(e.Format) == "":
		return fmt.Errorf("data stream format is required")
	case len(e.Format) > maxFormatBytes:
		return fmt.Errorf("data stream format is too long")
	case len(e.Payload) > maxPayloadBytes:
		return fmt.Errorf("data stream payload is too large: %d bytes", len(e.Payload))
	case e.Flags&^allEnvelopeFlags != 0:
		return fmt.Errorf("data stream envelope has unknown flags: %d", e.Flags&^allEnvelopeFlags)
	}

	switch e.PayloadType {
	case PayloadFrame:
		if e.Flags&FlagCommit != 0 {
			return fmt.Errorf("frame payloads are implicitly committed")
		}
		if e.FirstIndex != 0 || e.ItemCount != 0 || e.TotalItemCount != 0 {
			return fmt.Errorf("frame payload has BlockSet indexing")
		}
		if len(e.Payload) == 0 {
			return fmt.Errorf("frame payload is empty")
		}
	case PayloadBlockSet:
		if e.Flags&FlagRecovery != 0 {
			return fmt.Errorf("BlockSet payload cannot be a recovery frame")
		}
		if e.TotalItemCount == 0 {
			return fmt.Errorf("BlockSet total item count must be non-zero")
		}
		if e.Flags&FlagCommit != 0 {
			if e.FirstIndex != 0 || e.ItemCount != 0 || len(e.Payload) != 0 {
				return fmt.Errorf("BlockSet commit must not contain a range or payload")
			}
			return nil
		}
		if e.ItemCount == 0 {
			return fmt.Errorf("BlockSet data item count must be non-zero")
		}
		if uint64(e.FirstIndex)+uint64(e.ItemCount) > uint64(e.TotalItemCount) {
			return fmt.Errorf("BlockSet range exceeds total item count")
		}
		if len(e.Payload) == 0 {
			return fmt.Errorf("BlockSet data payload is empty")
		}
	default:
		return fmt.Errorf("unknown data stream payload type %d", e.PayloadType)
	}
	return nil
}

// Encode serializes one validated envelope.
func Encode(envelope Envelope) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, encodedSize(envelope)))
	if err := Write(buf, envelope); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Write serializes one validated envelope directly to w. Publishers should
// prefer this over Encode when their transport exposes a streaming message
// writer, avoiding an additional full-payload allocation and copy.
func Write(w io.Writer, envelope Envelope) error {
	if err := envelope.Validate(defaultMaxPayload); err != nil {
		return err
	}
	if len(envelope.StreamID) > math.MaxUint16 || len(envelope.Format) > math.MaxUint16 {
		return fmt.Errorf("data stream string exceeds wire length")
	}
	if _, err := io.WriteString(w, wireMagic); err != nil {
		return err
	}
	fields := []any{
		byte(envelope.PayloadType),
		byte(envelope.Flags),
		uint16(0),
		envelope.Generation,
		envelope.Sequence,
		envelope.TimestampUnixNano,
		envelope.FrameID,
		envelope.FirstIndex,
		envelope.ItemCount,
		envelope.TotalItemCount,
		uint16(len(envelope.StreamID)),
		uint16(len(envelope.Format)),
		uint32(len(envelope.Payload)),
	}
	for _, field := range fields {
		if err := binary.Write(w, binary.LittleEndian, field); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(w, envelope.StreamID); err != nil {
		return err
	}
	if _, err := io.WriteString(w, envelope.Format); err != nil {
		return err
	}
	_, err := w.Write(envelope.Payload)
	return err
}

// Decode deserializes and validates one complete envelope.
func Decode(data []byte, maxPayloadBytes int) (Envelope, error) {
	return decode(data, maxPayloadBytes, true)
}

// DecodeBorrowed deserializes and validates one complete envelope without
// copying its payload. The returned Payload aliases data and is only valid
// while the caller keeps data alive and immutable.
func DecodeBorrowed(data []byte, maxPayloadBytes int) (Envelope, error) {
	return decode(data, maxPayloadBytes, false)
}

func decode(data []byte, maxPayloadBytes int, copyPayload bool) (Envelope, error) {
	if len(data) < fixedHeaderSize {
		return Envelope{}, fmt.Errorf("data stream envelope is truncated")
	}
	if string(data[:len(wireMagic)]) != wireMagic {
		return Envelope{}, fmt.Errorf("unsupported data stream wire format")
	}
	reader := bytes.NewReader(data[len(wireMagic):])

	payloadType, err := reader.ReadByte()
	if err != nil {
		return Envelope{}, err
	}
	flags, err := reader.ReadByte()
	if err != nil {
		return Envelope{}, err
	}
	var reserved uint16
	var envelope Envelope
	var streamIDLength uint16
	var formatLength uint16
	var payloadLength uint32
	fields := []any{
		&reserved,
		&envelope.Generation,
		&envelope.Sequence,
		&envelope.TimestampUnixNano,
		&envelope.FrameID,
		&envelope.FirstIndex,
		&envelope.ItemCount,
		&envelope.TotalItemCount,
		&streamIDLength,
		&formatLength,
		&payloadLength,
	}
	for _, field := range fields {
		if err := binary.Read(reader, binary.LittleEndian, field); err != nil {
			return Envelope{}, fmt.Errorf("decode data stream header: %w", err)
		}
	}
	if reserved != 0 {
		return Envelope{}, fmt.Errorf("data stream reserved header bits are non-zero")
	}
	if int(streamIDLength) > maxStreamIDBytes || int(formatLength) > maxFormatBytes {
		return Envelope{}, fmt.Errorf("data stream string length exceeds limit")
	}
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = defaultMaxPayload
	}
	if uint64(payloadLength) > uint64(maxPayloadBytes) {
		return Envelope{}, fmt.Errorf("data stream payload length exceeds limit")
	}
	expectedRemaining := uint64(streamIDLength) + uint64(formatLength) + uint64(payloadLength)
	if uint64(reader.Len()) != expectedRemaining {
		return Envelope{}, fmt.Errorf(
			"data stream envelope length mismatch: have %d bytes, expected %d",
			reader.Len(),
			expectedRemaining,
		)
	}

	streamID := make([]byte, streamIDLength)
	if _, err := io.ReadFull(reader, streamID); err != nil {
		return Envelope{}, err
	}
	format := make([]byte, formatLength)
	if _, err := io.ReadFull(reader, format); err != nil {
		return Envelope{}, err
	}
	var payload []byte
	if payloadLength > 0 {
		if copyPayload {
			payload = make([]byte, payloadLength)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return Envelope{}, err
			}
		} else {
			payload = data[len(data)-int(payloadLength):]
		}
	}
	envelope.StreamID = string(streamID)
	envelope.Format = string(format)
	envelope.PayloadType = PayloadType(payloadType)
	envelope.Flags = Flags(flags)
	envelope.Payload = payload
	if err := envelope.Validate(maxPayloadBytes); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}
