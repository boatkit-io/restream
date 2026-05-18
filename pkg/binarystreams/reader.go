package binarystreams

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"unicode/utf16"
)

// Reader implements a bytestream reader.  For now it is LSB only.
type Reader struct {
	byteReader bufio.Reader
}

// NewReader returns a new Reader based on an io.Reader.  This will end up opening a bufio.Reader from the handed reader.
func NewReader(reader io.Reader) *Reader {
	return &Reader{
		byteReader: *bufio.NewReader(reader),
	}
}

// NewReaderFromBytes returns a new Reader based on a byte array
func NewReaderFromBytes(data []byte) *Reader {
	return NewReader(bytes.NewReader(data))
}

// NewReaderFromFile returns a new Reader based on using os.Open on the passed path.  It also returns the opened file object,
// for you to be able to close when done.
func NewReaderFromFile(path string) (*Reader, *os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return NewReader(f), f, nil
}

// ReadBytes reads a passed number of bytes from the stream and returns them as an array.  If anything other than the exact
// requested number of bytes are read/returned, an error will be returned instead.  ReadBytes handles re-issuing underlying
// IO reads to fill out the entire requested number of bytes.
func (r *Reader) ReadBytes(numBytes int) ([]byte, error) {
	if numBytes < 0 {
		return nil, fmt.Errorf("invalid length to ReadBytes: %d", numBytes)
	}

	b := make([]byte, numBytes)
	br, err := r.byteReader.Read(b)
	if err != nil {
		return nil, err
	}

	if br == numBytes {
		return b, nil
	}

	// Have to iterate multiple times, so it's more complicated now -- keep reading and bolting onto the end until we get all our bytes
	numBytes -= br
	b = b[0:br]

	for {
		bn := make([]byte, numBytes)
		br, err := r.byteReader.Read(bn)
		if err != nil {
			return nil, err
		}
		bn = bn[0:br]
		numBytes -= br
		b = append(b, bn...)

		if numBytes == 0 {
			return b, nil
		}
	}
}

// ReadStringToTerminator reads bytes until it runs into the passed terminator byte, and then returns everything up to (but not
// including) the terminator byte as a string.
func (r *Reader) ReadStringToTerminator(terminator byte) (string, uint32, error) {
	arr := make([]byte, 0)
	for {
		b, err := r.ReadByte()
		if err != nil {
			return "", 0, err
		}

		if b == terminator {
			return string(arr), uint32(len(arr) + 1), nil
		}

		arr = append(arr, b)
	}
}

// ReadWideStringToTerminator reads runes until it runs into the passed terminator rune, and then returns everything up to (but not
// including) the terminator rune as a string, as well as the length of bytes read.
func (r *Reader) ReadWideStringToTerminator(terminator uint16) (string, uint32, error) {
	arr := make([]uint16, 0)
	for {
		b, err := r.ReadUInt16()
		if err != nil {
			return "", 0, err
		}

		if b == terminator {
			return string(utf16.Decode(arr)), uint32(len(arr)*2 + 2), nil
		}

		arr = append(arr, b)
	}
}

// ReadString reads the passed number of bytes and returns them as a string.
func (r *Reader) ReadString(numChars int) (string, error) {
	b, err := r.ReadBytes(numChars)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ReadUint64String reads the passed number of bytes in as a string and then converts them to a Uint64 with strconv.ParseUint
// (assumes base 10).
func (r *Reader) ReadUint64String(numChars int) (uint64, error) {
	str, err := r.ReadString(numChars)
	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(str, 10, 64)
}

// PeekByte takes advantage of the bufio.Reader to read a byte without moving the underlying stream offset forward (usually called
// a "Peek" operation).
func (r *Reader) PeekByte() (byte, error) {
	b, err := r.byteReader.Peek(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

// ReadByte returns a single byte from the stream
func (r *Reader) ReadByte() (byte, error) {
	b, err := r.ReadBytes(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

// ReadUInt8 returns a single uint8 from the stream (same thing as ReadByte)
func (r *Reader) ReadUInt8() (uint8, error) {
	return r.ReadByte()
}

// ReadInt8 returns a single int8 from the stream
func (r *Reader) ReadInt8() (int8, error) {
	b, err := r.ReadBytes(1)
	if err != nil {
		return 0, err
	}
	return int8(b[0]), nil
}

// ReadUInt16 returns a single uint16 from the stream
func (r *Reader) ReadUInt16() (uint16, error) {
	b, err := r.ReadBytes(2)
	if err != nil {
		return 0, err
	}
	ret := uint16(b[0]) | (uint16(b[1]) << 8)
	return ret, nil
}

// ReadInt16 returns a single int16 from the stream
func (r *Reader) ReadInt16() (int16, error) {
	b, err := r.ReadBytes(2)
	if err != nil {
		return 0, err
	}
	ret := int16(b[0]) | (int16(b[1]) << 8)
	return ret, nil
}

// ReadUInt32 returns a single uint32 from the stream
func (r *Reader) ReadUInt32() (uint32, error) {
	b, err := r.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	ret := uint32(b[0]) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16) | (uint32(b[3]) << 24)
	return ret, nil
}

// ReadUInt64 returns a single uint64 from the stream
func (r *Reader) ReadUInt64() (uint64, error) {
	b, err := r.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	ret := uint64(b[0]) | (uint64(b[1]) << 8) | (uint64(b[2]) << 16) | (uint64(b[3]) << 24) |
		(uint64(b[4]) << 32) | (uint64(b[5]) << 40) | (uint64(b[6]) << 48) | (uint64(b[7]) << 56)
	return ret, nil
}

// ReadInt32 returns a single int32 from the stream
func (r *Reader) ReadInt32() (int32, error) {
	b, err := r.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	ret := int32(b[0]) | (int32(b[1]) << 8) | (int32(b[2]) << 16) | (int32(b[3]) << 24)
	return ret, nil
}

// ReadFloat32 reads a single float32 from the stream
func (r *Reader) ReadFloat32() (float32, error) {
	u, err := r.ReadUInt32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(u), nil
}

// ReadFloat64 reads a single float64 from the stream
func (r *Reader) ReadFloat64() (float64, error) {
	u, err := r.ReadUInt64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(u), nil
}

// Slice reads a byte array of the passed length and builds a sub-reader out of it
func (r *Reader) Slice(count int) (*Reader, error) {
	b, err := r.ReadBytes(count)
	if err != nil {
		return nil, err
	}
	return NewReaderFromBytes(b), nil
}

// SkipBytes skips the read offset forward a number of bytes
func (r *Reader) SkipBytes(count int) error {
	_, err := r.byteReader.Discard(count)
	return err
}

// IsEOF returns whether the reader is at the end of the buffer
func (r *Reader) IsEOF() bool {
	_, err := r.byteReader.Peek(1)
	return errors.Is(err, io.EOF)
}
