package binarystreams

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
)

// Writer implements a bytestream writer.  For now it is LSB only.
type Writer struct {
	byteWriter *bufio.Writer
}

// NewWriter returns a Writer object from an io.Writer.  Note: This will end up building a bufio.Writer from it.
func NewWriter(writer io.Writer) *Writer {
	return &Writer{
		byteWriter: bufio.NewWriter(writer),
	}
}

// NewMemoryWriter returns a new Writer that writes to a bytes.Buffer
func NewMemoryWriter() (*Writer, *bytes.Buffer) {
	var b bytes.Buffer
	bw := bufio.NewWriter(&b)
	return NewWriter(bw), &b
}

// NewWriterToFile builds a new Writer by calling os.Create on the passed path and handing it to NewWriter.  This also returns
// the underlying *os.File for the caller to manually close when complete.
func NewWriterToFile(path string) (*Writer, *os.File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return NewWriter(f), f, nil
}

// Flush flushes the underlying bufio.Writer stream, so call this before closing a file, if you are using a file stream.
func (w *Writer) Flush() error {
	return w.byteWriter.Flush()
}

// WriteBytes writes all of the passed bytes to the underlying stream, erroring if anything other than the entire slice was
// written out successfully.
func (w *Writer) WriteBytes(bytes []byte) error {
	bw, err := w.byteWriter.Write(bytes)
	if err != nil {
		return err
	}
	if bw != len(bytes) {
		return fmt.Errorf("not all bytes written (expected %d, wrote %d)", len(bytes), bw)
	}
	return nil
}

// WriteUint64String takes a passed (uint64) number, converts it to a string, zero-pads it to make it numChars long, and then
// writes it out to the underlying stream (passes through to WriteBytes, in fact).  If the stringified-number is longer than
// numChars characters, it will also error -- the intent of numChars is to write out exactly numChars bytes, no more, no fewer.
func (w *Writer) WriteUint64String(num uint64, numChars int) error {
	str := strconv.FormatUint(num, 10)
	numChars -= len(str)
	if numChars < 0 {
		return fmt.Errorf("string longer than allowed number of characters")
	}
	for i := 0; i < numChars; i++ {
		err := w.WriteByte('0')
		if err != nil {
			return err
		}
	}
	return w.WriteBytes([]byte(str))
}

// WriteByte writes a single passed byte to the underlying stream.
func (w *Writer) WriteByte(b byte) error {
	return w.WriteBytes([]byte{b})
}

// WriteInt8 writes a single passed int8 to the underlying stream.
func (w *Writer) WriteInt8(i int8) error {
	return w.WriteBytes([]byte{byte(i)})
}

// WriteUInt8 writes a single passed uint8 to the underlying stream.
func (w *Writer) WriteUInt8(i uint8) error {
	return w.WriteBytes([]byte{i})
}

// WriteInt16 writes a single passed int16 to the underlying stream.
func (w *Writer) WriteInt16(i int16) error {
	b := []byte{byte(i & 0xff), byte(i >> 8)}
	return w.WriteBytes(b)
}

// WriteUInt16 writes a single passed uint16 to the underlying stream.
func (w *Writer) WriteUInt16(i uint16) error {
	b := []byte{byte(i & 0xff), byte(i >> 8)}
	return w.WriteBytes(b)
}

// WriteInt32 writes a single passed int32 to the underlying stream.
func (w *Writer) WriteInt32(i int32) error {
	b := []byte{byte(i & 0xff), byte((i >> 8) & 0xff), byte((i >> 16) & 0xff), byte((i >> 24) & 0xff)}
	return w.WriteBytes(b)
}

// WriteUInt32 writes a single passed uint32 to the underlying stream.
func (w *Writer) WriteUInt32(i uint32) error {
	b := []byte{byte(i & 0xff), byte((i >> 8) & 0xff), byte((i >> 16) & 0xff), byte((i >> 24) & 0xff)}
	return w.WriteBytes(b)
}

// WriteUInt64 writes a single passed uint64 to the underlying stream.
func (w *Writer) WriteUInt64(i uint64) error {
	b := []byte{byte(i & 0xff), byte((i >> 8) & 0xff), byte((i >> 16) & 0xff), byte((i >> 24) & 0xff),
		byte((i >> 32) & 0xff), byte((i >> 40) & 0xff), byte((i >> 48) & 0xff), byte((i >> 56) & 0xff)}
	return w.WriteBytes(b)
}

// WriteFloat32 writes a float32, in big-endian
func (w *Writer) WriteFloat32(v float32) error {
	return w.WriteUInt32(math.Float32bits(v))
}

// WriteFloat64 writes a float64, in big-endian
func (w *Writer) WriteFloat64(v float64) error {
	return w.WriteUInt64(math.Float64bits(v))
}
