// Package protocol defines the binary packet protocol used between a device server and a relay server.
//
// Standard packet kinds occupy the low range. Applications can either send fixed raw packets in the
// FirstApplicationPacketKind..KindCustom range, or use KindCustom with a namespaced string name and opaque payload.
// Unknown packet kinds decode as RawPacket so newer peers can be ignored or routed by older peers.
package protocol

// CurrentVersion is the current relay protocol version.
const CurrentVersion uint32 = 5

// PacketKind identifies the type of a relay packet.
type PacketKind byte

const (
	// KindConnected is sent by the relay server after a device hello is accepted.
	KindConnected PacketKind = iota + 1
	// KindFullState carries a full serialized store state.
	KindFullState
	// KindPartialState carries a serialized store partial.
	KindPartialState
	// KindEvent carries a serialized Restream event.
	KindEvent
	// KindRPCCall carries an RPC call from the relay server down to the device server.
	KindRPCCall
	// KindRPCResponse carries an RPC response from the device server back to the relay server.
	KindRPCResponse
)

const (
	// FirstApplicationPacketKind is the first fixed packet kind reserved for application-defined extensions.
	FirstApplicationPacketKind PacketKind = 128
	// KindCustom carries a named application-defined extension packet with an opaque payload.
	KindCustom PacketKind = 255
)

// DeviceHello is the first message a device sends after opening the relay websocket.
//
// AuthType and AuthData are intentionally opaque to keep authentication policy outside the protocol codec.
type DeviceHello struct {
	ProtocolVersion uint32
	DeviceID        string
	AuthType        string
	AuthData        []byte
	Metadata        map[string]string
}

// Packet is implemented by all decoded relay packets.
type Packet interface {
	Kind() PacketKind
}

// ConnectedPacket acknowledges an accepted device connection.
type ConnectedPacket struct {
	ProtocolVersion uint32
	Metadata        map[string]string
}

// Kind implements Packet.
func (*ConnectedPacket) Kind() PacketKind {
	return KindConnected
}

// StoreStatePacket carries either full store state or a store partial.
type StoreStatePacket struct {
	PacketKind PacketKind
	StoreName  string
	Data       []byte
}

// NewFullStatePacket creates a full-state store packet.
func NewFullStatePacket(storeName string, state []byte) *StoreStatePacket {
	return &StoreStatePacket{PacketKind: KindFullState, StoreName: storeName, Data: state}
}

// NewPartialStatePacket creates a partial-state store packet.
func NewPartialStatePacket(storeName string, partial []byte) *StoreStatePacket {
	return &StoreStatePacket{PacketKind: KindPartialState, StoreName: storeName, Data: partial}
}

// Kind implements Packet.
func (p *StoreStatePacket) Kind() PacketKind {
	return p.PacketKind
}

// EventPacket carries a serialized Restream event.
type EventPacket struct {
	EventName string
	Data      []byte
}

// Kind implements Packet.
func (*EventPacket) Kind() PacketKind {
	return KindEvent
}

// RPCCallPacket carries an RPC call from the relay server to the device server.
type RPCCallPacket struct {
	RPCID       uint32
	MethodName  string
	AccessLevel byte
	Request     []byte
}

// Kind implements Packet.
func (*RPCCallPacket) Kind() PacketKind {
	return KindRPCCall
}

// RPCResponsePacket carries an RPC response from the device server to the relay server.
type RPCResponsePacket struct {
	RPCID    uint32
	Response []byte
}

// Kind implements Packet.
func (*RPCResponsePacket) Kind() PacketKind {
	return KindRPCResponse
}

// CustomPacket is the preferred extension point for application-defined packets.
//
// Name should be namespaced by convention, for example "com.example.metrics".
// Payload is opaque to the relay protocol package.
type CustomPacket struct {
	Name    string
	Payload []byte
}

// Kind implements Packet.
func (*CustomPacket) Kind() PacketKind {
	return KindCustom
}

// RawPacket represents a fixed application-defined packet or a decoded unknown future packet kind.
type RawPacket struct {
	PacketKind PacketKind
	Payload    []byte
}

// Kind implements Packet.
func (p *RawPacket) Kind() PacketKind {
	return p.PacketKind
}

// IsStandardKind reports whether kind is reserved by this protocol package.
func IsStandardKind(kind PacketKind) bool {
	return kind >= KindConnected && kind <= KindRPCResponse
}

// IsApplicationKind reports whether kind is in the fixed application extension range.
func IsApplicationKind(kind PacketKind) bool {
	return kind >= FirstApplicationPacketKind && kind < KindCustom
}
