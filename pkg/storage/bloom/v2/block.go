package bloom

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/grafana/loki/v3/pkg/storage/bloom/shared"
	"github.com/grafana/loki/v3/pkg/util/encoding"
)

const MagicNumber = 0x626F6F6D

type Block struct {
	Header Header
	Blooms Blooms
	Index  Index
	Footer Footer
}

var _ Serializable[*Block] = &Block{}

// Decode implements Serializable.
func (b *Block) Decode(dec *encoding.Decbuf, v Version) error {
	sig := dec.Be32()
	if sig != MagicNumber {
		return fmt.Errorf("invalid magic number: got %x, expected %x", sig, MagicNumber)
	}

	if err := b.Header.Decode(dec, v); err != nil {
		return errors.Wrap(err, "decoding header")
	}

	if err := b.Blooms.Decode(dec, v); err != nil {
		return errors.Wrap(err, "decoding blooms")
	}

	indexLen := len(dec.B) - b.Footer.Len() // remove last 20 bytes that contain the footer
	indexDec := encoding.DecWith(dec.B[:indexLen])
	if err := b.Index.Decode(&indexDec, v); err != nil {
		return errors.Wrap(err, "decoding index")
	}

	dec.B = dec.B[indexLen:] // skip bytes from the index
	if err := b.Footer.Decode(dec, v); err != nil {
		return errors.Wrap(err, "decoding footer")
	}

	return dec.Err()
}

func (b *Block) ReadFrom(buf []byte) error {
	dec := encoding.DecWith(buf)
	return b.Decode(&dec, 0)
}

// Encode implements Serializable.
func (b *Block) Encode(enc *encoding.Encbuf, v Version) {
	enc.PutBE32(MagicNumber)

	b.Header.Encode(enc, v)
	b.Blooms.Encode(enc, v)

	// encode index with separate encoder so we can calculate the checksum correctly
	indexEnc := encoding.EncWith(make([]byte, 0, 4<<10))
	b.Index.Encode(&indexEnc, v)
	enc.PutBytes(indexEnc.Get())

	// add checksum to footer if not set before encoding it
	if b.Footer.Checksum == 0 {
		crc32Hash := shared.Crc32HashPool.Get()
		defer shared.Crc32HashPool.Put(crc32Hash)
		crc32Hash.Write(enc.Get())
		b.Footer.Checksum = crc32Hash.Sum32()
	}
	b.Footer.Encode(enc, v)
}
