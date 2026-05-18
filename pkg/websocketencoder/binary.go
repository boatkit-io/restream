package websocketencoder

import (
	"bytes"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/zishang520/socket.io/parsers/socket/v3/parser"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

// Placeholder
type Placeholder struct {
	Placeholder bool `json:"_placeholder" mapstructure:"_placeholder" msgpack:"_placeholder"`
	Num         int  `json:"num" mapstructure:"num" msgpack:"num"`
}

func init() { //nolint:gochecknoinits
	jsoniter.RegisterTypeEncoderFunc("types.BytesBuffer", func(ptr unsafe.Pointer, stream *jsoniter.Stream) {
		bb := ((*types.BytesBuffer)(ptr))

		bufList := stream.Attachment.([]types.BufferInterface)
		_placeholder := &Placeholder{Placeholder: true, Num: len(bufList)}
		stream.WriteVal(_placeholder)
		stream.Attachment = append(bufList, bb) //nolint:gocritic
	}, nil)

	jsoniter.RegisterTypeEncoderFunc("[]uint8", func(ptr unsafe.Pointer, stream *jsoniter.Stream) {
		bb := types.NewBytesBuffer(nil)
		barr := ((*[]byte)(ptr))
		bb.Write(*barr) //nolint:errcheck

		bufList := stream.Attachment.([]types.BufferInterface)
		_placeholder := &Placeholder{Placeholder: true, Num: len(bufList)}
		stream.WriteVal(_placeholder)
		stream.Attachment = append(bufList, bb) //nolint:gocritic
	}, nil)
}

// DeconstructPacket Replaces every io.Reader | []byte in packet with a numbered placeholder.
func DeconstructPacket(packet *parser.Packet) (pack *parser.Packet, buffers []types.BufferInterface) {
	packetCopy := *packet
	pack = &packetCopy

	// Run the serialization now, replacing any bytebuffers/[]byte found along the way with placeholders
	buf := &bytes.Buffer{}
	ns := jsoniter.NewStream(jsoniter.ConfigDefault, buf, buf.Cap())
	ns.Attachment = buffers
	ns.WriteVal(pack.Data)
	buffers = ns.Attachment.([]types.BufferInterface)
	ns.Flush()
	pack.Data = preSerializedData(buf.String())

	attachments := uint64(len(buffers))
	pack.Attachments = &attachments // number of binary 'attachments'
	return pack, buffers
}
