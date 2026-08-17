// Package protocol defines the binary packet protocol used between a device server and a relay server.
//
// Standard packet kinds occupy the low range. Applications can either send fixed raw packets in the
// FirstApplicationPacketKind..KindCustom range, or use KindCustom with a namespaced string name and opaque payload.
// Unknown packet kinds decode as RawPacket so newer peers can be ignored or routed by older peers.
package protocol

// CurrentVersion changes only when the fundamental framing or processing model
// becomes incompatible. Optional packet kinds and capabilities do not bump it;
// unknown packet kinds are decoded as RawPacket so mixed-version peers can keep
// the base relay online.
const CurrentVersion uint32 = 8

const (
	// DeviceDataStreamsCapabilityMetadataKey advertises support for asynchronous
	// data-stream subscription packets and completion results.
	DeviceDataStreamsCapabilityMetadataKey = "restream.capability.data-streams"
	// DeviceRPCAnnotationsCapabilityMetadataKey advertises support for optional
	// key/value annotations appended to RPC call packets.
	DeviceRPCAnnotationsCapabilityMetadataKey = "restream.capability.rpc-annotations"
	enabledCapabilityMetadataValue            = "1"
)

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
	// KindStoreSubscription carries a store keyed-subscription lifecycle change from the relay server to the device server.
	KindStoreSubscription
	// KindKeyedEvent carries a serialized store-owned keyed event from the device server to the relay server.
	KindKeyedEvent
	// KindKeyedEventSubscription carries a keyed-event lifecycle change from the relay server to the device server.
	KindKeyedEventSubscription
	// KindFFRPCCall carries a fire-and-forget RPC call from the relay server down to the device server.
	KindFFRPCCall
	// KindDataStreamSubscription carries a high-bandwidth stream lifecycle change from the relay server to the device.
	KindDataStreamSubscription
)

const (
	// KindDataStreamSubscriptionResult acknowledges an asynchronous data-stream
	// lifecycle change. Its explicit value preserves all previously assigned
	// packet-kind numbers.
	KindDataStreamSubscriptionResult PacketKind = 12
	// KindDataStreamSubscriptionRequest carries the capability-gated,
	// acknowledged form of a stream lifecycle change without altering the
	// original optional packet's wire layout.
	KindDataStreamSubscriptionRequest PacketKind = 13
	// KindRelayRPCCall carries an RPC call from a device to its relay server.
	KindRelayRPCCall PacketKind = 14
	// KindRelayRPCResponse carries the relay server response to a device-originated RPC.
	KindRelayRPCResponse PacketKind = 15
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

// DeviceCapabilities are optional extensions advertised in DeviceHello
// metadata without changing the base protocol version.
type DeviceCapabilities struct {
	DataStreams    bool
	RPCAnnotations bool
}

// CapabilitiesFromDeviceMetadata decodes Restream-reserved device capability
// keys while leaving application metadata opaque.
func CapabilitiesFromDeviceMetadata(metadata map[string]string) DeviceCapabilities {
	return DeviceCapabilities{
		DataStreams:    metadata[DeviceDataStreamsCapabilityMetadataKey] == enabledCapabilityMetadataValue,
		RPCAnnotations: metadata[DeviceRPCAnnotationsCapabilityMetadataKey] == enabledCapabilityMetadataValue,
	}
}

// DeviceMetadataWithCapabilities clones application metadata and adds Restream
// capability keys.
func DeviceMetadataWithCapabilities(
	metadata map[string]string,
	capabilities DeviceCapabilities,
) map[string]string {
	ret := make(map[string]string, len(metadata)+2)
	for key, value := range metadata {
		if key != DeviceDataStreamsCapabilityMetadataKey && key != DeviceRPCAnnotationsCapabilityMetadataKey {
			ret[key] = value
		}
	}
	if capabilities.DataStreams {
		ret[DeviceDataStreamsCapabilityMetadataKey] = enabledCapabilityMetadataValue
	}
	if capabilities.RPCAnnotations {
		ret[DeviceRPCAnnotationsCapabilityMetadataKey] = enabledCapabilityMetadataValue
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

// Packet is implemented by all decoded relay packets.
type Packet interface {
	Kind() PacketKind
}

// RelayCapabilities advertises optional relay-server behavior to a connected device.
type RelayCapabilities struct {
	OnDemandStoreStreaming bool
	RelayRPCs              bool
}

// ConnectedPacket acknowledges an accepted device connection.
type ConnectedPacket struct {
	ProtocolVersion uint32
	Capabilities    RelayCapabilities
	// Metadata contains application-defined relay metadata. Restream capabilities are exposed separately.
	Metadata map[string]string
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

// KeyedEventPacket carries a serialized store-owned event for one exact subscription key.
type KeyedEventPacket struct {
	StoreName string
	EventName string
	Key       string
	Data      []byte
}

// Kind implements Packet.
func (*KeyedEventPacket) Kind() PacketKind {
	return KindKeyedEvent
}

// RPCCallPacket carries an RPC call from the relay server to the device server.
type RPCCallPacket struct {
	RPCID       uint32
	MethodName  string
	AccessLevel byte
	Request     []byte
	Annotations map[string]string
}

// FFRPCCallPacket carries an FFRPC call from the relay server to the device
// server without a response ID.
type FFRPCCallPacket struct {
	MethodName  string
	AccessLevel byte
	Request     []byte
	Annotations map[string]string
}

// Kind implements Packet.
func (*FFRPCCallPacket) Kind() PacketKind {
	return KindFFRPCCall
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

// RelayRPCCallPacket carries an RPC call from an authenticated device to its relay server.
type RelayRPCCallPacket struct {
	RPCID      uint32
	MethodName string
	Request    []byte
}

// Kind implements Packet.
func (*RelayRPCCallPacket) Kind() PacketKind {
	return KindRelayRPCCall
}

// RelayRPCResponsePacket carries a response to a device-originated RPC.
type RelayRPCResponsePacket struct {
	RPCID    uint32
	Response []byte
	Error    string
}

// Kind implements Packet.
func (*RelayRPCResponsePacket) Kind() PacketKind {
	return KindRelayRPCResponse
}

// Kind implements Packet.
func (*RPCResponsePacket) Kind() PacketKind {
	return KindRPCResponse
}

// StoreSubscriptionAction identifies the lifecycle action for a relayed store subscription.
type StoreSubscriptionAction byte

const (
	// StoreSubscribe starts a keyed subscription on the device side.
	StoreSubscribe StoreSubscriptionAction = iota
	// StoreUnsubscribe stops a keyed subscription on the device side.
	StoreUnsubscribe
)

// StoreSubscriptionPacket carries a keyed store subscription lifecycle change from the relay server to the device.
type StoreSubscriptionPacket struct {
	StoreName string
	Key       string
	Action    StoreSubscriptionAction
}

// Kind implements Packet.
func (*StoreSubscriptionPacket) Kind() PacketKind {
	return KindStoreSubscription
}

// KeyedEventSubscriptionPacket carries a keyed-event subscription lifecycle change from the relay server to the device.
type KeyedEventSubscriptionPacket struct {
	StoreName string
	EventName string
	Key       string
	Action    StoreSubscriptionAction
}

// Kind implements Packet.
func (*KeyedEventSubscriptionPacket) Kind() PacketKind {
	return KindKeyedEventSubscription
}

// DataStreamSubscriptionPacket carries a high-bandwidth stream subscription
// lifecycle change from the relay server to the device. Payload bytes travel
// over the separate data-plane endpoint, never this control connection.
type DataStreamSubscriptionPacket struct {
	StoreName  string
	StreamName string
	Key        string
	Action     StoreSubscriptionAction
}

// Kind implements Packet.
func (*DataStreamSubscriptionPacket) Kind() PacketKind {
	return KindDataStreamSubscription
}

// DataStreamSubscriptionRequestPacket carries an independently authorized,
// asynchronously acknowledged source transition.
type DataStreamSubscriptionRequestPacket struct {
	OperationID uint32
	StoreName   string
	StreamName  string
	Key         string
	AccessLevel byte
	Action      StoreSubscriptionAction
}

// Kind implements Packet.
func (*DataStreamSubscriptionRequestPacket) Kind() PacketKind {
	return KindDataStreamSubscriptionRequest
}

// DataStreamSubscriptionResultPacket reports completion of one asynchronous
// source startup or shutdown. Error is empty on success.
type DataStreamSubscriptionResultPacket struct {
	OperationID uint32
	Error       string
}

// Kind implements Packet.
func (*DataStreamSubscriptionResultPacket) Kind() PacketKind {
	return KindDataStreamSubscriptionResult
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
	return kind >= KindConnected && kind <= KindRelayRPCResponse
}

// IsApplicationKind reports whether kind is in the fixed application extension range.
func IsApplicationKind(kind PacketKind) bool {
	return kind >= FirstApplicationPacketKind && kind < KindCustom
}
