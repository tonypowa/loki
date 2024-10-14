package bloom

import (
	"github.com/grafana/loki/v3/pkg/compression"
	"github.com/grafana/loki/v3/pkg/util/encoding"
)

type Version byte

const (
	Unsupported Version = iota
	V1                  // First version of v2 Block
)

var _ Serializable[*Header] = &Header{}

type Header struct {
	Version  Version
	Encoding compression.Codec
}

// Decode implements Serializable.
func (h *Header) Decode(enc *encoding.Decbuf, _ Version) error {
	h.Version = Version(enc.Byte())
	h.Encoding = compression.Codec(enc.Byte())
	return enc.Err()
}

// Encode implements Serializable.
func (h *Header) Encode(enc *encoding.Encbuf, _ Version) {
	enc.PutByte(byte(h.Version))
	enc.PutByte(byte(h.Encoding))
}
