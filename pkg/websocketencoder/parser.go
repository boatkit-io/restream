package websocketencoder

import (
	"github.com/zishang520/socket.io-go-parser/v2/parser"
)

// customParser is a struct
type customParser struct{}

// NewEncoder is a func
func (p *customParser) NewEncoder() parser.Encoder {
	return NewEncoder()
}

// NewDecoder  is a func
func (p *customParser) NewDecoder() parser.Decoder {
	return parser.NewDecoder()
}

// NewParser  is a func
func NewParser() parser.Parser {
	return &customParser{}
}
