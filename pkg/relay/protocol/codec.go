package protocol

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

const (
	maxUint16 = int(^uint16(0))
	maxUint32 = uint64(^uint32(0))
)

// EncodeDeviceHello serializes a device hello message.
func EncodeDeviceHello(hello *DeviceHello) ([]byte, error) {
	if hello == nil {
		return nil, fmt.Errorf("nil device hello")
	}

	w, buf := newMemoryWriter()
	if err := w.WriteUInt32(hello.ProtocolVersion); err != nil {
		return nil, err
	}
	if err := writeString(w, hello.DeviceID); err != nil {
		return nil, err
	}
	if err := writeString(w, hello.AuthType); err != nil {
		return nil, err
	}
	if err := writeBytes(w, hello.AuthData); err != nil {
		return nil, err
	}
	if err := writeStringMap(w, hello.Metadata); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeDeviceHello deserializes a device hello message.
func DecodeDeviceHello(data []byte) (*DeviceHello, error) {
	r := binarystreams.NewReaderFromBytes(data)

	protocolVersion, err := r.ReadUInt32()
	if err != nil {
		return nil, err
	}
	deviceID, err := readString(r)
	if err != nil {
		return nil, err
	}
	authType, err := readString(r)
	if err != nil {
		return nil, err
	}
	authData, err := readBytes(r)
	if err != nil {
		return nil, err
	}
	metadata, err := readStringMap(r)
	if err != nil {
		return nil, err
	}
	if err := ensureEOF(r, "device hello"); err != nil {
		return nil, err
	}

	return &DeviceHello{
		ProtocolVersion: protocolVersion,
		DeviceID:        deviceID,
		AuthType:        authType,
		AuthData:        authData,
		Metadata:        metadata,
	}, nil
}

// EncodePacket serializes one relay packet. The returned bytes are intended to be sent as one websocket message.
func EncodePacket(packet Packet) ([]byte, error) {
	if packet == nil {
		return nil, fmt.Errorf("nil packet")
	}

	w, buf := newMemoryWriter()
	if err := w.WriteByte(byte(packet.Kind())); err != nil {
		return nil, err
	}

	switch p := packet.(type) {
	case *ConnectedPacket:
		if err := encodeConnectedPacket(w, p); err != nil {
			return nil, err
		}
	case *StoreStatePacket:
		if err := encodeStoreStatePacket(w, p); err != nil {
			return nil, err
		}
	case *EventPacket:
		if err := encodeNamedData(w, p.EventName, p.Data); err != nil {
			return nil, err
		}
	case *RPCCallPacket:
		if err := encodeRPCCallPacket(w, p); err != nil {
			return nil, err
		}
	case *RPCResponsePacket:
		if err := encodeRPCResponsePacket(w, p); err != nil {
			return nil, err
		}
	case *StoreSubscriptionPacket:
		if err := encodeStoreSubscriptionPacket(w, p); err != nil {
			return nil, err
		}
	case *CustomPacket:
		if err := encodeCustomPacket(w, p); err != nil {
			return nil, err
		}
	case *RawPacket:
		if err := encodeRawPacket(w, p); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported packet type %T", packet)
	}

	if err := w.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodePacket deserializes one relay packet from a single websocket message.
func DecodePacket(data []byte) (Packet, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty packet")
	}

	kind := PacketKind(data[0])
	if kind == 0 {
		return nil, fmt.Errorf("packet kind is zero")
	}
	body := data[1:]
	r := binarystreams.NewReaderFromBytes(body)

	switch kind {
	case KindConnected:
		packet, err := decodeConnectedPacket(r)
		if err != nil {
			return nil, err
		}
		return packet, ensureEOF(r, "connected packet")
	case KindFullState, KindPartialState:
		packet, err := decodeStoreStatePacket(kind, r)
		if err != nil {
			return nil, err
		}
		return packet, ensureEOF(r, "store state packet")
	case KindEvent:
		name, payload, err := decodeNamedData(r)
		if err != nil {
			return nil, err
		}
		return &EventPacket{EventName: name, Data: payload}, ensureEOF(r, "event packet")
	case KindRPCCall:
		packet, err := decodeRPCCallPacket(r)
		if err != nil {
			return nil, err
		}
		return packet, ensureEOF(r, "rpc call packet")
	case KindRPCResponse:
		packet, err := decodeRPCResponsePacket(r)
		if err != nil {
			return nil, err
		}
		return packet, ensureEOF(r, "rpc response packet")
	case KindStoreSubscription:
		packet, err := decodeStoreSubscriptionPacket(r)
		if err != nil {
			return nil, err
		}
		return packet, ensureEOF(r, "store subscription packet")
	case KindCustom:
		packet, err := decodeCustomPacket(r)
		if err != nil {
			return nil, err
		}
		return packet, ensureEOF(r, "custom packet")
	default:
		return &RawPacket{PacketKind: kind, Payload: append([]byte(nil), body...)}, nil
	}
}

func encodeConnectedPacket(w *binarystreams.Writer, packet *ConnectedPacket) error {
	if err := w.WriteUInt32(packet.ProtocolVersion); err != nil {
		return err
	}
	return writeStringMap(w, packet.Metadata)
}

func decodeConnectedPacket(r *binarystreams.Reader) (*ConnectedPacket, error) {
	protocolVersion, err := r.ReadUInt32()
	if err != nil {
		return nil, err
	}
	metadata, err := readStringMap(r)
	if err != nil {
		return nil, err
	}
	return &ConnectedPacket{ProtocolVersion: protocolVersion, Metadata: metadata}, nil
}

func encodeStoreStatePacket(w *binarystreams.Writer, packet *StoreStatePacket) error {
	switch packet.PacketKind {
	case KindFullState, KindPartialState:
	default:
		return fmt.Errorf("store state packet has invalid kind %d", packet.PacketKind)
	}
	return encodeNamedData(w, packet.StoreName, packet.Data)
}

func decodeStoreStatePacket(kind PacketKind, r *binarystreams.Reader) (*StoreStatePacket, error) {
	storeName, data, err := decodeNamedData(r)
	if err != nil {
		return nil, err
	}
	return &StoreStatePacket{PacketKind: kind, StoreName: storeName, Data: data}, nil
}

func encodeRPCCallPacket(w *binarystreams.Writer, packet *RPCCallPacket) error {
	if err := w.WriteUInt32(packet.RPCID); err != nil {
		return err
	}
	if err := writeString(w, packet.MethodName); err != nil {
		return err
	}
	if err := w.WriteByte(packet.AccessLevel); err != nil {
		return err
	}
	return writeBytes(w, packet.Request)
}

func decodeRPCCallPacket(r *binarystreams.Reader) (*RPCCallPacket, error) {
	rpcID, err := r.ReadUInt32()
	if err != nil {
		return nil, err
	}
	methodName, err := readString(r)
	if err != nil {
		return nil, err
	}
	accessLevel, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	request, err := readBytes(r)
	if err != nil {
		return nil, err
	}
	return &RPCCallPacket{
		RPCID:       rpcID,
		MethodName:  methodName,
		AccessLevel: accessLevel,
		Request:     request,
	}, nil
}

func encodeRPCResponsePacket(w *binarystreams.Writer, packet *RPCResponsePacket) error {
	if err := w.WriteUInt32(packet.RPCID); err != nil {
		return err
	}
	return writeBytes(w, packet.Response)
}

func decodeRPCResponsePacket(r *binarystreams.Reader) (*RPCResponsePacket, error) {
	rpcID, err := r.ReadUInt32()
	if err != nil {
		return nil, err
	}
	response, err := readBytes(r)
	if err != nil {
		return nil, err
	}
	return &RPCResponsePacket{RPCID: rpcID, Response: response}, nil
}

func encodeStoreSubscriptionPacket(w *binarystreams.Writer, packet *StoreSubscriptionPacket) error {
	if err := writeString(w, packet.StoreName); err != nil {
		return err
	}
	if err := writeString(w, packet.Key); err != nil {
		return err
	}
	switch packet.Action {
	case StoreSubscribe, StoreUnsubscribe:
	default:
		return fmt.Errorf("invalid store subscription action %d", packet.Action)
	}
	return w.WriteByte(byte(packet.Action))
}

func decodeStoreSubscriptionPacket(r *binarystreams.Reader) (*StoreSubscriptionPacket, error) {
	storeName, err := readString(r)
	if err != nil {
		return nil, err
	}
	key, err := readString(r)
	if err != nil {
		return nil, err
	}
	actionByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	action := StoreSubscriptionAction(actionByte)
	switch action {
	case StoreSubscribe, StoreUnsubscribe:
	default:
		return nil, fmt.Errorf("invalid store subscription action %d", action)
	}
	return &StoreSubscriptionPacket{
		StoreName: storeName,
		Key:       key,
		Action:    action,
	}, nil
}

func encodeCustomPacket(w *binarystreams.Writer, packet *CustomPacket) error {
	if packet.Name == "" {
		return fmt.Errorf("custom packet name is empty")
	}
	return encodeNamedData(w, packet.Name, packet.Payload)
}

func decodeCustomPacket(r *binarystreams.Reader) (*CustomPacket, error) {
	name, payload, err := decodeNamedData(r)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("custom packet name is empty")
	}
	return &CustomPacket{Name: name, Payload: payload}, nil
}

func encodeRawPacket(w *binarystreams.Writer, packet *RawPacket) error {
	if !IsApplicationKind(packet.PacketKind) {
		return fmt.Errorf("raw packet kind %d is outside the application-defined range", packet.PacketKind)
	}
	return w.WriteBytes(packet.Payload)
}

func encodeNamedData(w *binarystreams.Writer, name string, data []byte) error {
	if err := writeString(w, name); err != nil {
		return err
	}
	return writeBytes(w, data)
}

func decodeNamedData(r *binarystreams.Reader) (string, []byte, error) {
	name, err := readString(r)
	if err != nil {
		return "", nil, err
	}
	data, err := readBytes(r)
	if err != nil {
		return "", nil, err
	}
	return name, data, nil
}

func writeString(w *binarystreams.Writer, value string) error {
	if len(value) > maxUint16 {
		return fmt.Errorf("string length %d exceeds %d bytes", len(value), maxUint16)
	}
	if err := w.WriteUInt16(uint16(len(value))); err != nil {
		return err
	}
	return w.WriteBytes([]byte(value))
}

func readString(r *binarystreams.Reader) (string, error) {
	length, err := r.ReadUInt16()
	if err != nil {
		return "", err
	}
	return r.ReadString(int(length))
}

func writeBytes(w *binarystreams.Writer, value []byte) error {
	if uint64(len(value)) > maxUint32 {
		return fmt.Errorf("byte slice length %d exceeds %d bytes", len(value), maxUint32)
	}
	if err := w.WriteUInt32(uint32(len(value))); err != nil {
		return err
	}
	return w.WriteBytes(value)
}

func readBytes(r *binarystreams.Reader) ([]byte, error) {
	length, err := r.ReadUInt32()
	if err != nil {
		return nil, err
	}
	return r.ReadBytes(int(length))
}

func writeStringMap(w *binarystreams.Writer, values map[string]string) error {
	if len(values) > maxUint16 {
		return fmt.Errorf("map length %d exceeds %d entries", len(values), maxUint16)
	}
	if err := w.WriteUInt16(uint16(len(values))); err != nil {
		return err
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if err := writeString(w, key); err != nil {
			return err
		}
		if err := writeString(w, values[key]); err != nil {
			return err
		}
	}
	return nil
}

func readStringMap(r *binarystreams.Reader) (map[string]string, error) {
	count, err := r.ReadUInt16()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	values := make(map[string]string, count)
	for range count {
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		value, err := readString(r)
		if err != nil {
			return nil, err
		}
		values[key] = value
	}
	return values, nil
}

func ensureEOF(r *binarystreams.Reader, context string) error {
	if !r.IsEOF() {
		return fmt.Errorf("%s has trailing bytes", context)
	}
	return nil
}

func newMemoryWriter() (*binarystreams.Writer, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return binarystreams.NewWriter(buf), buf
}
