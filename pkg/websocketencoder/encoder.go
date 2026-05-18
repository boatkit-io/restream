package websocketencoder

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/zishang520/socket.io/parsers/socket/v3/parser"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

type preSerializedData string

// encoder A socket.io Encoder instance
type encoder struct {
}

// NewEncoder is an encoder
func NewEncoder() parser.Encoder {
	return &encoder{}
}

// Encode a packet as a single string if non-binary, or as a
// buffer sequence, depending on packet type.
func (e *encoder) Encode(packet *parser.Packet) []types.BufferInterface {
	if packet.Type == parser.EVENT || packet.Type == parser.ACK {
		if HasBinary(packet.Data) {
			data := *packet
			if packet.Type == parser.EVENT {
				data.Type = parser.BINARY_EVENT
			} else {
				data.Type = parser.BINARY_ACK
			}
			return e.encodeAsBinary(&data)
		}
	}
	return []types.BufferInterface{e.encodeAsString(packet)}
}

// _encodeData is a func
func _encodeData(data any) any {
	switch tdata := data.(type) {
	case nil:
		return nil
	// *strings.Reader special handling
	case *strings.Reader:
		rdata, _ := types.NewStringBufferReader(tdata) //nolint:errcheck
		return rdata
	case []any:
		newData := make([]any, 0, len(tdata))
		for _, v := range tdata {
			newData = append(newData, _encodeData(v))
		}
		return newData
	case map[string]any:
		newData := make(map[string]any, len(tdata))
		for k, v := range tdata {
			newData[k] = _encodeData(v)
		}
		return newData
	}

	return data
}

// encodeAsString Encode packet as string.
func (e *encoder) encodeAsString(packet *parser.Packet) types.BufferInterface {
	// first is type
	str := types.NewStringBuffer([]byte{byte(packet.Type) + '0'})
	// attachments if we have them
	if (packet.Type == parser.BINARY_EVENT || packet.Type == parser.BINARY_ACK) && packet.Attachments != nil {
		str.WriteString(strconv.FormatUint(*packet.Attachments, 10)) //nolint:errcheck
		str.WriteByte('-')                                           //nolint:errcheck
	}
	// if we have a namespace other than `/`
	// we append it followed by a comma `,`
	if len(packet.Nsp) > 0 && packet.Nsp != "/" {
		str.WriteString(packet.Nsp) //nolint:errcheck
		str.WriteByte(',')          //nolint:errcheck
	}
	// immediately followed by the id
	if nil != packet.Id {
		str.WriteString(strconv.FormatUint(*packet.Id, 10)) //nolint:errcheck
	}
	// json data
	if nil != packet.Data {
		if pds, is := packet.Data.(preSerializedData); is {
			// Already serialized in the DeconstructPacket function
			str.WriteString(string(pds)) //nolint:errcheck
		} else {
			if b, err := json.Marshal(_encodeData(packet.Data)); err == nil {
				str.Write(b) //nolint:errcheck
			}
		}
	}
	return str
}

// encodeAsBinary Encode packet as 'buffer sequence' by removing blobs, and
// deconstructing packet into object with placeholders and
// a list of buffers.
func (e *encoder) encodeAsBinary(obj *parser.Packet) []types.BufferInterface {
	packet, buffers := DeconstructPacket(obj)
	return append([]types.BufferInterface{e.encodeAsString(packet)}, buffers...) // write all the buffers
}
