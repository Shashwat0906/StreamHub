// Package protocol defines StreamHub's binary wire protocol: frame layout,
// API keys, error codes and the request/response messages.
//
// Encoding rules (all big-endian):
//
//	int8/16/32/64      fixed width
//	string             uint16 length + UTF-8 bytes
//	bytes              int32 length + bytes (-1 = nil)
//	array              int32 count + elements
//
// Every message implements Encode(*Encoder) and Decode(*Decoder). Decoding
// uses a "sticky error": after the first failure every read returns zero
// values and Err() reports the failure, which keeps message decoders short.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrShortBuffer is reported when a message ends early.
var ErrShortBuffer = errors.New("protocol: short buffer")

// Encoder appends encoded values to a byte slice.
type Encoder struct {
	buf []byte
}

// NewEncoder returns an encoder whose output starts with prefix (callers
// use this to reserve space for a frame header).
func NewEncoder(prefix []byte) *Encoder { return &Encoder{buf: prefix} }

// Bytes returns the encoded data.
func (e *Encoder) Bytes() []byte { return e.buf }

func (e *Encoder) Int8(v int8)   { e.buf = append(e.buf, byte(v)) }
func (e *Encoder) Bool(v bool)   { e.Int8(boolToInt8(v)) }
func (e *Encoder) Int16(v int16) { e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(v)) }
func (e *Encoder) Int32(v int32) { e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(v)) }
func (e *Encoder) Int64(v int64) { e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(v)) }
func (e *Encoder) Uint64(v uint64) {
	e.buf = binary.BigEndian.AppendUint64(e.buf, v)
}

func (e *Encoder) String(s string) {
	if len(s) > math.MaxUint16 {
		s = s[:math.MaxUint16]
	}
	e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(len(s)))
	e.buf = append(e.buf, s...)
}

func (e *Encoder) BytesField(b []byte) {
	if b == nil {
		e.Int32(-1)
		return
	}
	e.Int32(int32(len(b)))
	e.buf = append(e.buf, b...)
}

func (e *Encoder) ArrayLen(n int) { e.Int32(int32(n)) }

func (e *Encoder) Int32s(v []int32) {
	e.ArrayLen(len(v))
	for _, x := range v {
		e.Int32(x)
	}
}

func (e *Encoder) Strings(v []string) {
	e.ArrayLen(len(v))
	for _, x := range v {
		e.String(x)
	}
}

func (e *Encoder) StringMap(m map[string]string) {
	e.ArrayLen(len(m))
	for k, v := range m {
		e.String(k)
		e.String(v)
	}
}

func boolToInt8(b bool) int8 {
	if b {
		return 1
	}
	return 0
}

// Decoder reads values from a byte slice.
type Decoder struct {
	b   []byte
	off int
	err error
}

func NewDecoder(b []byte) *Decoder { return &Decoder{b: b} }

// Err returns the first decoding error, if any.
func (d *Decoder) Err() error { return d.err }

// Remaining returns the number of unread bytes.
func (d *Decoder) Remaining() int { return len(d.b) - d.off }

func (d *Decoder) need(n int) bool {
	if d.err != nil {
		return false
	}
	if n < 0 || d.off+n > len(d.b) {
		d.err = ErrShortBuffer
		return false
	}
	return true
}

func (d *Decoder) Int8() int8 {
	if !d.need(1) {
		return 0
	}
	v := int8(d.b[d.off])
	d.off++
	return v
}

func (d *Decoder) Bool() bool { return d.Int8() != 0 }

func (d *Decoder) Int16() int16 {
	if !d.need(2) {
		return 0
	}
	v := int16(binary.BigEndian.Uint16(d.b[d.off:]))
	d.off += 2
	return v
}

func (d *Decoder) Int32() int32 {
	if !d.need(4) {
		return 0
	}
	v := int32(binary.BigEndian.Uint32(d.b[d.off:]))
	d.off += 4
	return v
}

func (d *Decoder) Int64() int64 {
	if !d.need(8) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(d.b[d.off:]))
	d.off += 8
	return v
}

func (d *Decoder) Uint64() uint64 { return uint64(d.Int64()) }

func (d *Decoder) String() string {
	if !d.need(2) {
		return ""
	}
	n := int(binary.BigEndian.Uint16(d.b[d.off:]))
	d.off += 2
	if !d.need(n) {
		return ""
	}
	s := string(d.b[d.off : d.off+n])
	d.off += n
	return s
}

// BytesField returns a copy of a length-prefixed byte array.
func (d *Decoder) BytesField() []byte {
	n := d.Int32()
	if d.err != nil {
		return nil
	}
	if n == -1 {
		return nil
	}
	if !d.need(int(n)) {
		return nil
	}
	out := make([]byte, n)
	copy(out, d.b[d.off:d.off+int(n)])
	d.off += int(n)
	return out
}

// ArrayLen reads an array length and sanity-checks it against the bytes
// left (each element needs at least minElemSize bytes), so a corrupt
// length cannot make us allocate gigabytes.
func (d *Decoder) ArrayLen(minElemSize int) int {
	n := d.Int32()
	if d.err != nil {
		return 0
	}
	if n < 0 {
		return 0
	}
	if minElemSize < 1 {
		minElemSize = 1
	}
	if int64(n)*int64(minElemSize) > int64(d.Remaining()) {
		d.err = fmt.Errorf("protocol: array length %d exceeds buffer", n)
		return 0
	}
	return int(n)
}

func (d *Decoder) Int32s() []int32 {
	n := d.ArrayLen(4)
	if n == 0 {
		return nil
	}
	out := make([]int32, n)
	for i := range out {
		out[i] = d.Int32()
	}
	return out
}

func (d *Decoder) Strings() []string {
	n := d.ArrayLen(2)
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = d.String()
	}
	return out
}

func (d *Decoder) StringMap() map[string]string {
	n := d.ArrayLen(4)
	if n == 0 {
		return nil
	}
	m := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := d.String()
		m[k] = d.String()
	}
	return m
}

// Message is implemented by every request and response.
type Message interface {
	Encode(e *Encoder)
	Decode(d *Decoder)
}

// Marshal encodes a message into a fresh byte slice.
func Marshal(m Message) []byte {
	e := NewEncoder(make([]byte, 0, 128))
	m.Encode(e)
	return e.Bytes()
}

// Unmarshal decodes b into m and requires that the whole buffer is used.
func Unmarshal(b []byte, m Message) error {
	d := NewDecoder(b)
	m.Decode(d)
	if d.err != nil {
		return d.err
	}
	if d.Remaining() != 0 {
		return fmt.Errorf("protocol: %d trailing bytes", d.Remaining())
	}
	return nil
}
