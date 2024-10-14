package bloom

import (
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/loki/v3/pkg/storage/bloom/shared"
	"github.com/grafana/loki/v3/pkg/util/encoding"
)

var _ Serializable[*Blooms] = &Blooms{}

type Blooms struct {
	Pages []shared.Bloom
}

// Decode implements Serializable.
func (b *Blooms) Decode(dec *encoding.Decbuf, v Version) error {
	n := dec.Be64()
	b.Pages = make([]shared.Bloom, n)
	for x := range b.Pages {
		if err := b.Pages[x].Decode(dec); err != nil {
			return err
		}
	}
	return dec.Err()
}

// Encode implements Serializable.
func (b *Blooms) Encode(enc *encoding.Encbuf, v Version) {
	_ = b.EncodeWithErrorHandling(enc, v)
}

func (b *Blooms) EncodeWithErrorHandling(enc *encoding.Encbuf, v Version) error {
	var err multierror.MultiError
	enc.PutBE64(uint64(len(b.Pages)))
	for x := range b.Pages {
		err.Add(b.Pages[x].Encode(enc))
	}
	return err.Err()
}
