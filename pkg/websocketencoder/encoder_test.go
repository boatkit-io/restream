package websocketencoder

import (
	"bytes"
	"testing"

	"github.com/zishang520/socket.io/parsers/socket/v3/parser"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

type testBinaryPayload struct {
	Name string                `json:"name"`
	Data types.BufferInterface `json:"data"`
	Raw  []byte                `json:"raw"`
}

func TestEncoderDeconstructsStructBinaryFields(t *testing.T) {
	enc := NewEncoder()

	buffers := enc.Encode(&parser.Packet{
		Type: parser.EVENT,
		Data: []any{
			"storeupdate",
			testBinaryPayload{
				Name: "payload",
				Data: types.NewBytesBuffer([]byte{1, 2, 3}),
				Raw:  []byte{4, 5, 6},
			},
		},
	})

	if len(buffers) != 3 {
		t.Fatalf("expected header plus two binary buffers, got %d", len(buffers))
	}

	header := string(buffers[0].Bytes())
	expectedHeader := `52-["storeupdate",{"name":"payload","data":{"_placeholder":true,"num":0},"raw":{"_placeholder":true,"num":1}}]`
	if header != expectedHeader {
		t.Fatalf("unexpected header:\n got: %s\nwant: %s", header, expectedHeader)
	}

	if !bytes.Equal(buffers[1].Bytes(), []byte{1, 2, 3}) {
		t.Fatalf("unexpected first binary buffer: %v", buffers[1].Bytes())
	}
	if !bytes.Equal(buffers[2].Bytes(), []byte{4, 5, 6}) {
		t.Fatalf("unexpected second binary buffer: %v", buffers[2].Bytes())
	}
}

func TestEncoderQuotesPlainStringData(t *testing.T) {
	enc := NewEncoder()

	buffers := enc.Encode(&parser.Packet{
		Type: parser.EVENT,
		Data: "plain-string",
	})

	if len(buffers) != 1 {
		t.Fatalf("expected one buffer, got %d", len(buffers))
	}

	got := string(buffers[0].Bytes())
	const expected = `2"plain-string"`
	if got != expected {
		t.Fatalf("unexpected encoded string data: got %s want %s", got, expected)
	}
}
