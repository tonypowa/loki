package bloom

import (
	"github.com/prometheus/common/model"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/storage/bloom/shared"
	"github.com/grafana/loki/v3/pkg/util/encoding"
)

type Offset struct {
	Offset uint64
	Len    uint64
}

var _ Serializable[*Offset] = &Offset{}
var _ DeltaSerializable[*Offset] = &Offset{}

// Decode implements Serializable.
func (o *Offset) Decode(dec *encoding.Decbuf, v Version) error {
	o.Offset = dec.Uvarint64()
	o.Len = dec.Uvarint64()
	return dec.Err()
}

// DecodeDelta implements DeltaSerializable.
func (o *Offset) DecodeDelta(dec *encoding.Decbuf, v Version, prev *Offset) error {
	if prev == nil {
		o.Offset = dec.Uvarint64()
	} else {
		o.Offset = prev.Offset + dec.Uvarint64()
	}
	o.Len = dec.Uvarint64()
	return dec.Err()
}

// Encode implements Serializable.
func (o *Offset) Encode(enc *encoding.Encbuf, v Version) {
	enc.PutUvarint64(o.Offset)
	enc.PutUvarint64(o.Len)
}

// EncodeDelta implements DeltaSerializable.
func (o *Offset) EncodeDelta(enc *encoding.Encbuf, v Version, prev *Offset) {
	// delta encode byte offset
	if prev == nil {
		enc.PutUvarint64(o.Offset)
	} else {
		enc.PutUvarint64(o.Offset - prev.Offset)
	}
	enc.PutUvarint64(o.Len)
}

type ChunkRef logproto.ShortRef

var _ Serializable[*ChunkRef] = &ChunkRef{}
var _ DeltaSerializable[*ChunkRef] = &ChunkRef{}

// Encode implements Serializable.
func (r *ChunkRef) Encode(enc *encoding.Encbuf, v Version) {
	enc.PutVarint64(int64(r.From))
	enc.PutVarint64(int64(r.Through - r.From))
	enc.PutBE32(r.Checksum)
}

// EncodeDelta implements DeltaSerializable.
func (r *ChunkRef) EncodeDelta(enc *encoding.Encbuf, _ Version, prev *ChunkRef) {
	// delta encode start time
	if prev == nil {
		enc.PutVarint64(int64(r.From))
	} else {
		enc.PutVarint64(int64(r.From - prev.From))
	}
	// delta encode end time
	enc.PutVarint64(int64(r.Through - r.From))
	enc.PutBE32(r.Checksum)
}

// Decode implements Serializable.
func (r *ChunkRef) Decode(dec *encoding.Decbuf, _ Version) error {
	r.From = model.Time(dec.Varint64())
	r.Through = r.From + model.Time(dec.Varint64())
	r.Checksum = dec.Be32()
	return dec.Err()
}

// DecodeDelta implements DeltaSerializable.
func (r *ChunkRef) DecodeDelta(dec *encoding.Decbuf, _ Version, prev *ChunkRef) error {
	if prev == nil {
		r.From = model.Time(dec.Varint64())
	} else {
		r.From = prev.From + model.Time(dec.Varint64())
	}
	r.Through = r.From + model.Time(dec.Varint64())
	r.Checksum = dec.Be32()
	return dec.Err()
}

var _ Serializable[*Index] = &Index{}

type Index struct {
	Fingerprint model.Fingerprint
	Chunks      []ChunkRef
	Offsets     []Offset
	Fields      shared.Set[string]
	// Checksum uint32 is not part of the struct, but only present in binary data
}

// Decode implements Serializable.
func (i *Index) Decode(dec *encoding.Decbuf, v Version) error {
	// CheckCrc reads the last 4 bytes of dec as uint32 and compares it the
	// checksum of the rest of the buffer.
	// It also removes the last 4 bytes from the dec buffer.
	if err := dec.CheckCrc(shared.CastagnoliTable); err != nil {
		return err
	}

	i.Fingerprint = model.Fingerprint(dec.Uvarint64())

	lenChunks := dec.Uvarint()
	i.Chunks = make([]ChunkRef, lenChunks)
	var prevChunk *ChunkRef
	for x := range i.Chunks {
		i.Chunks[x].DecodeDelta(dec, v, prevChunk)
		prevChunk = &i.Chunks[x]
	}

	lenOffsets := dec.Uvarint()
	i.Offsets = make([]Offset, lenOffsets)
	var prevOffset *Offset
	for x := range i.Offsets {
		i.Offsets[x].DecodeDelta(dec, v, prevOffset)
		prevOffset = &i.Offsets[x]
	}

	lenFields := dec.Uvarint()
	i.Fields = shared.NewSet[string](lenFields)
	for x := 0; x < lenFields; x++ {
		i.Fields.Add(dec.UvarintStr())
	}

	return dec.Err()
}

// Encode implements Serializable.
func (i *Index) Encode(enc *encoding.Encbuf, v Version) {
	enc.PutUvarint64(uint64(i.Fingerprint))

	enc.PutUvarint(len(i.Chunks))
	var prevChunk *ChunkRef
	for x := range i.Chunks {
		i.Chunks[x].EncodeDelta(enc, v, prevChunk)
		prevChunk = &i.Chunks[x]
	}

	enc.PutUvarint(len(i.Offsets))
	var prevOffset *Offset
	for x := range i.Offsets {
		i.Offsets[x].EncodeDelta(enc, v, prevOffset)
		prevOffset = &i.Offsets[x]
	}

	enc.PutUvarint(i.Fields.Len())
	for _, v := range i.Fields.Items() {
		enc.PutUvarintStr(v)
	}

	crc32Hash := shared.Crc32HashPool.Get()
	defer shared.Crc32HashPool.Put(crc32Hash)
	enc.PutHash(crc32Hash)
}
