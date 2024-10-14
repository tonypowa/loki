package shared

import (
	"bytes"

	"github.com/grafana/loki/v3/pkg/storage/bloom/v1/filter"
	"github.com/grafana/loki/v3/pkg/util/encoding"
	"github.com/pkg/errors"
)

type Bloom struct {
	filter.ScalableBloomFilter
}

func NewBloom() *Bloom {
	return &Bloom{
		// TODO parameterise SBF options. fp_rate
		ScalableBloomFilter: *filter.NewScalableBloomFilter(1024, 0.01, 0.8),
	}
}

func (b *Bloom) Encode(enc *encoding.Encbuf) error {
	// divide by 8 b/c bloom capacity is measured in bits, but we want bytes
	buf := bytes.NewBuffer(make([]byte, 0, int(b.Capacity()/8)))

	// TODO(owen-d): have encoder implement writer directly so we don't need
	// to indirect via a buffer
	_, err := b.WriteTo(buf)
	if err != nil {
		return errors.Wrap(err, "encoding bloom filter")
	}

	data := buf.Bytes()
	enc.PutUvarint(len(data)) // length of bloom filter
	enc.PutBytes(data)
	return nil
}

func (b *Bloom) DecodeCopy(dec *encoding.Decbuf) error {
	ln := dec.Uvarint()
	data := dec.Bytes(ln)

	_, err := b.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "decoding copy of bloom filter")
	}

	return nil
}

func (b *Bloom) Decode(dec *encoding.Decbuf) error {
	ln := dec.Uvarint()
	data := dec.Bytes(ln)

	_, err := b.DecodeFrom(data)
	if err != nil {
		return errors.Wrap(err, "decoding bloom filter")
	}

	return nil
}
