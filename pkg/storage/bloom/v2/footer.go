package bloom

import (
	"github.com/grafana/loki/v3/pkg/util/encoding"
)

type Footer struct {
	IndexOffset uint64
	IndexLen    uint64
	Checksum    uint32
}

var _ Serializable[*Footer] = &Footer{}

func (f *Footer) Len() int {
	return 8 + 8 + 4
}

// Decode implements Serializable.
func (f *Footer) Decode(dec *encoding.Decbuf, v Version) error {
	f.IndexOffset = dec.Be64()
	f.IndexLen = dec.Be64()
	f.Checksum = dec.Be32() // TODO: checksum is not validated yet
	return dec.Err()
}

// Encode implements Serializable.
func (f *Footer) Encode(enc *encoding.Encbuf, v Version) {
	enc.PutBE64(f.IndexOffset)
	enc.PutBE64(f.IndexLen)
	enc.PutBE32(f.Checksum)
}
