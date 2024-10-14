package bloom

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/compression"
	"github.com/grafana/loki/v3/pkg/storage/bloom/shared"
	"github.com/grafana/loki/v3/pkg/util/encoding"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestBloomDecoder(t *testing.T) {
	v := V1

	blk := Block{
		Header: Header{
			Version:  v,
			Encoding: compression.None,
		},
		Blooms: Blooms{
			Pages: []shared.Bloom{ // 1 byte for uvarint64(len(Pages))
				*shared.NewBloom(), // 1464 bytes
				*shared.NewBloom(), // 1464 bytes
			},
		},
		Index: Index{
			Fingerprint: model.Fingerprint(0xafbfcfdf),
			Chunks: []ChunkRef{
				ChunkRef{From: 0, Through: 1000, Checksum: 123},
				ChunkRef{From: 500, Through: 1500, Checksum: 234},
			},
			Offsets: []Offset{
				Offset{Offset: 4 + 1 + 1 + 8, Len: 1464},
				Offset{Offset: 4 + 1 + 1 + 8 + 1464, Len: 1464},
			},
			Fields: shared.NewSetFromLiteral("field_a", "field_b", "field_c"),
		},
		Footer: Footer{
			IndexOffset: 4 + 1 + 1 + 8 + 1464 + 1464,
			IndexLen:    58,
		},
	}

	enc := encoding.EncWith(make([]byte, 0, 4<<10))
	blk.Encode(&enc, v)
	t.Log(len(enc.B), "bytes")

	blkDec, err := NewInMemoryDecoder(enc.B)
	require.NoError(t, err)

	for blkDec.Next() {
		sbf := blkDec.At()
		require.NotNil(t, sbf)
		require.NoError(t, blkDec.Err())
		t.Log(sbf)
	}

	require.NoError(t, blkDec.Err())
}
