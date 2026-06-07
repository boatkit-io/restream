// Package server accepts device connections and relays Restream packets into application sessions.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
	gws "github.com/gorilla/websocket"
)

const (
	defaultRPCWriteTimeout       = 5 * time.Second
	defaultCloseWriteTimeout     = time.Second
	maxWebsocketCloseReasonBytes = 123
	relayServerShutdownReason    = "relay server shutting down"
)

// AuthFunc authenticates a device hello and returns the relay access level for the connection.
type AuthFunc func(context.Context, *protocol.DeviceHello, *Connection) (restream.AccessLevel, error)

// Config configures a device relay server.
type Config struct {
	DeviceManager      *DeviceManager
	AuthenticateDevice AuthFunc
	Metadata           map[string]string
	RPCWriteTimeout    time.Duration
}

// Server accepts device relay websocket connections.
type Server struct {
	config Config
}

// New creates a device relay server.
func New(config Config) *Server {
	if config.RPCWriteTimeout <= 0 {
		config.RPCWriteTimeout = defaultRPCWriteTimeout
	}
	return &Server{config: config}
}

// AcceptConn handles a device websocket until it disconnects or ctx is cancelled.
func (s *Server) AcceptConn(ctx context.Context, conn *gws.Conn) (retErr error) {
	if s.config.DeviceManager == nil {
		return fmt.Errorf("relay server DeviceManager is not configured")
	}
	if s.config.AuthenticateDevice == nil {
		return fmt.Errorf("relay server AuthenticateDevice is not configured")
	}

	conn.EnableWriteCompression(true)
	c := &Connection{
		conn:            conn,
		rpcWriteTimeout: s.config.RPCWriteTimeout,
		streamStarted:   time.Now(),
	}
	closeOnReturn := true
	defer func() {
		if closeOnReturn {
			closeConnectionForReturn(c, retErr)
		}
	}()

	closeOnContextDone := make(chan struct{})
	defer close(closeOnContextDone)
	go func() {
		select {
		case <-ctx.Done():
			c.CloseWithReason(gws.CloseGoingAway, relayServerShutdownReason) //nolint:errcheck // Why: Closing best effort to unblock ReadMessage.
		case <-closeOnContextDone:
		}
	}()

	device, err := s.acceptHello(ctx, c)
	if err != nil {
		return err
	}

	if err := s.sendConnected(c); err != nil {
		return err
	}

	device.DeviceConnected(c)
	defer device.DeviceDisconnected(c)
	defer func() {
		closeOnReturn = false
		closeConnectionForReturn(c, retErr)
	}()

	if err := device.sendActiveStoreSubscriptions(c); err != nil {
		return err
	}

	return s.readPackets(c, device)
}

func (s *Server) acceptHello(ctx context.Context, c *Connection) (*Device, error) {
	mt, message, err := c.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if mt != gws.BinaryMessage {
		return nil, fmt.Errorf("expected binary connection message, got %d", mt)
	}

	hello, err := protocol.DecodeDeviceHello(message)
	if err != nil {
		return nil, err
	}
	if hello.ProtocolVersion != protocol.CurrentVersion {
		return nil, fmt.Errorf("unsupported relay protocol version: %d", hello.ProtocolVersion)
	}

	c.DeviceID = hello.DeviceID
	accessLevel, err := s.config.AuthenticateDevice(ctx, hello, c)
	if err != nil {
		return nil, err
	}
	c.AccessLevel = accessLevel

	device, err := s.config.DeviceManager.GetDevice(hello.DeviceID)
	if err != nil {
		return nil, err
	}
	if device == nil {
		return nil, fmt.Errorf("relay server DeviceManager returned nil device")
	}

	return device, nil
}

func (s *Server) sendConnected(c *Connection) error {
	connectedBytes, err := protocol.EncodePacket(&protocol.ConnectedPacket{
		ProtocolVersion: protocol.CurrentVersion,
		Metadata:        s.config.Metadata,
	})
	if err != nil {
		return err
	}
	return c.conn.WriteMessage(gws.BinaryMessage, connectedBytes)
}

func (s *Server) readPackets(c *Connection, device *Device) error {
	for {
		mt, message, err := c.conn.ReadMessage()
		if err != nil {
			return err
		}
		if mt != gws.BinaryMessage {
			return fmt.Errorf("expected binary relay packet, got %d", mt)
		}

		packet, err := protocol.DecodePacket(message)
		if err != nil {
			return err
		}

		switch packet := packet.(type) {
		case *protocol.StoreStatePacket:
			switch packet.Kind() {
			case protocol.KindFullState:
				if err := device.HandleFullState(c, packet.StoreName, packet.Data); err != nil {
					return err
				}
			case protocol.KindPartialState:
				if err := device.HandlePartialState(c, packet.StoreName, packet.Data); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unhandled store packet type %d", packet.Kind())
			}
		case *protocol.EventPacket:
			if err := device.HandleEvent(c, packet.EventName, packet.Data); err != nil {
				return err
			}
		case *protocol.RPCCallPacket:
			return fmt.Errorf("device sent RPCCall packet")
		case *protocol.RPCResponsePacket:
			if err := device.HandleRPCResponse(c, packet.RPCID, packet.Response); err != nil {
				return err
			}
		case *protocol.CustomPacket:
			if err := device.HandleCustomPacket(c, packet); err != nil {
				return err
			}
		case *protocol.RawPacket:
			if err := device.HandleRawPacket(c, packet); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unhandled packet type %T", packet)
		}
	}
}

// Connection represents one authenticated device websocket.
type Connection struct {
	DeviceID    string
	AccessLevel restream.AccessLevel

	conn            *gws.Conn
	rpcWriteMutex   sync.Mutex
	rpcWriteTimeout time.Duration

	streamStarted   time.Time
	packetCount     uint64
	lastPacketType  string
	lastStoreName   string
	lastPacketBytes int
	lastPacketAt    time.Time
}

// NewConnection creates a Connection around an existing websocket.
func NewConnection(conn *gws.Conn) *Connection {
	return &Connection{conn: conn, rpcWriteTimeout: defaultRPCWriteTimeout, streamStarted: time.Now()}
}

// RemoteAddr returns the underlying websocket remote address.
func (c *Connection) RemoteAddr() net.Addr {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.RemoteAddr()
}

// Close closes the underlying websocket.
func (c *Connection) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// CloseWithReason sends a websocket close control frame with reason before closing the connection.
func (c *Connection) CloseWithReason(code int, reason string) error {
	if c == nil || c.conn == nil {
		return nil
	}

	var retErr error
	deadline := time.Now().Add(defaultCloseWriteTimeout)
	msg := gws.FormatCloseMessage(code, truncateWebsocketCloseReason(reason))

	c.rpcWriteMutex.Lock()
	if err := c.conn.WriteControl(gws.CloseMessage, msg, deadline); err != nil {
		retErr = err
	}
	c.rpcWriteMutex.Unlock()

	if err := c.conn.Close(); retErr == nil {
		retErr = err
	}
	return retErr
}

// SendRPC sends an RPC command to the connected device.
func (c *Connection) SendRPC(rpcID uint32, name string, accessLevel restream.AccessLevel, binaryData []byte) error {
	packetBytes, err := protocol.EncodePacket(&protocol.RPCCallPacket{
		RPCID:       rpcID,
		MethodName:  name,
		AccessLevel: byte(accessLevel),
		Request:     binaryData,
	})
	if err != nil {
		return err
	}

	return c.sendPacket(packetBytes)
}

// SendStoreSubscription sends a keyed store subscription lifecycle change to the connected device.
func (c *Connection) SendStoreSubscription(storeName string, key string, subscribe bool) error {
	action := protocol.StoreUnsubscribe
	if subscribe {
		action = protocol.StoreSubscribe
	}
	packetBytes, err := protocol.EncodePacket(&protocol.StoreSubscriptionPacket{
		StoreName: storeName,
		Key:       key,
		Action:    action,
	})
	if err != nil {
		return err
	}

	return c.sendPacket(packetBytes)
}

func (c *Connection) sendPacket(packetBytes []byte) error {
	c.rpcWriteMutex.Lock()
	defer c.rpcWriteMutex.Unlock()

	deadline := time.Now().Add(c.rpcWriteTimeout)
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return c.conn.WriteMessage(gws.BinaryMessage, packetBytes)
}

// RecordPacket records packet telemetry and returns fields suitable for logging.
func (c *Connection) RecordPacket(packetType string, storeName string, byteCount int) map[string]any {
	c.packetCount++
	c.lastPacketType = packetType
	c.lastStoreName = storeName
	c.lastPacketBytes = byteCount
	c.lastPacketAt = time.Now()

	return map[string]any{
		"deviceID":   c.ID(),
		"store":      storeName,
		"packetType": packetType,
		"bytes":      byteCount,
		"packetSeq":  c.packetCount,
	}
}

// Summary returns connection stream telemetry suitable for disconnect logging.
func (c *Connection) Summary() map[string]any {
	fields := map[string]any{
		"deviceID":        c.ID(),
		"packetCount":     c.packetCount,
		"lastPacketType":  c.lastPacketType,
		"lastStore":       c.lastStoreName,
		"lastPacketBytes": c.lastPacketBytes,
	}
	if !c.streamStarted.IsZero() {
		fields["streamDuration"] = time.Since(c.streamStarted).String()
	}
	if !c.lastPacketAt.IsZero() {
		fields["lastPacketAge"] = time.Since(c.lastPacketAt).String()
	}
	return fields
}

// ID returns a useful identifier for logs.
func (c *Connection) ID() string {
	if c.DeviceID != "" {
		return c.DeviceID
	}
	if c.RemoteAddr() != nil {
		return fmt.Sprintf("%+v", c.RemoteAddr())
	}
	return "unknown"
}

func shouldSendCloseReason(err error) bool {
	if err == nil {
		return false
	}

	var closeErr *gws.CloseError
	if errors.As(err, &closeErr) {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return false
	}
	return true
}

func closeConnectionForReturn(c *Connection, err error) {
	if shouldSendCloseReason(err) {
		c.CloseWithReason(gws.CloseInternalServerErr, err.Error()) //nolint:errcheck // Why: Already returning the stream error.
		return
	}
	c.Close() //nolint:errcheck // Why: Cleanup is best effort after the stream ends.
}

func truncateWebsocketCloseReason(reason string) string {
	if len(reason) <= maxWebsocketCloseReasonBytes {
		return reason
	}

	truncated := []byte(reason)
	truncated = truncated[:maxWebsocketCloseReasonBytes]
	for len(truncated) > 0 && !utf8.Valid(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return string(truncated)
}
