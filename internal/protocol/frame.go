package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MaxFrameSize bounds a single frame so a bad length cannot exhaust memory.
const MaxFrameSize = 64 << 20

const (
	requestHeaderSize  = 2 + 2 + 4 // api, version, correlation id
	responseHeaderSize = 4         // correlation id
)

// Version is the only protocol version spoken today.
const Version uint16 = 1

// RequestHeader precedes every request body.
type RequestHeader struct {
	API           APIKey
	Version       uint16
	CorrelationID uint32
}

// EncodeRequest builds a full request frame (length prefix included).
func EncodeRequest(api APIKey, corr uint32, m Message) []byte {
	e := NewEncoder(make([]byte, 4+requestHeaderSize, 256))
	binary.BigEndian.PutUint16(e.buf[4:], uint16(api))
	binary.BigEndian.PutUint16(e.buf[6:], Version)
	binary.BigEndian.PutUint32(e.buf[8:], corr)
	m.Encode(e)
	b := e.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))
	return b
}

// EncodeResponse builds a full response frame.
func EncodeResponse(corr uint32, m Message) []byte {
	e := NewEncoder(make([]byte, 4+responseHeaderSize, 256))
	binary.BigEndian.PutUint32(e.buf[4:], corr)
	m.Encode(e)
	b := e.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))
	return b
}

// ReadFrame reads one length-prefixed frame and returns its payload.
func ReadFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > MaxFrameSize {
		return nil, fmt.Errorf("protocol: frame of %d bytes exceeds max %d", n, MaxFrameSize)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// ParseRequest splits a request frame payload into header and body.
func ParseRequest(payload []byte) (RequestHeader, []byte, error) {
	if len(payload) < requestHeaderSize {
		return RequestHeader{}, nil, ErrShortBuffer
	}
	h := RequestHeader{
		API:           APIKey(binary.BigEndian.Uint16(payload)),
		Version:       binary.BigEndian.Uint16(payload[2:]),
		CorrelationID: binary.BigEndian.Uint32(payload[4:]),
	}
	return h, payload[requestHeaderSize:], nil
}

// ParseResponse splits a response frame payload into correlation ID and body.
func ParseResponse(payload []byte) (uint32, []byte, error) {
	if len(payload) < responseHeaderSize {
		return 0, nil, ErrShortBuffer
	}
	return binary.BigEndian.Uint32(payload), payload[responseHeaderSize:], nil
}
