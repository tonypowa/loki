package bloom

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/compression"
	"github.com/grafana/loki/v3/pkg/storage/bloom/shared"
	"github.com/grafana/loki/v3/pkg/util/encoding"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {

	v := V1

	t.Run("Header", func(t *testing.T) {
		h1 := Header{
			Version:  v,
			Encoding: compression.Snappy,
		}

		t.Run("default", func(t *testing.T) {
			enc := encoding.EncWith(make([]byte, 0, 128))
			h1.Encode(&enc, v)
			t.Log("encoded len", enc.Len())

			h2 := Header{}
			dec := encoding.DecWith(enc.Get())
			h2.Decode(&dec, v)

			require.Equal(t, h1, h2)
		})
	})

	t.Run("Offset", func(t *testing.T) {
		o1 := Offset{
			Offset: 1234,
			Len:    2345,
		}
		o2 := Offset{
			Offset: 3579,
			Len:    1234,
		}

		t.Run("default", func(t *testing.T) {
			enc := encoding.EncWith(make([]byte, 0, 128))
			o2.Encode(&enc, v)
			t.Log("encoded len", enc.Len())

			o3 := Offset{}
			dec := encoding.DecWith(enc.Get())
			o3.Decode(&dec, v)

			require.Equal(t, o2, o3)
		})

		t.Run("delta", func(t *testing.T) {
			enc := encoding.EncWith(make([]byte, 0, 128))
			o2.EncodeDelta(&enc, v, &o1)
			t.Log("encoded len", enc.Len())

			o3 := Offset{}
			dec := encoding.DecWith(enc.Get())
			o3.DecodeDelta(&dec, v, &o1)

			require.Equal(t, o2, o3)
		})
	})

	t.Run("ChunkRef", func(t *testing.T) {

		chk1 := ChunkRef{
			From:     100,
			Through:  300,
			Checksum: 123,
		}
		chk2 := ChunkRef{
			From:     200,
			Through:  400,
			Checksum: 234,
		}

		t.Run("default", func(t *testing.T) {
			enc := encoding.EncWith(make([]byte, 0, 128))
			chk2.Encode(&enc, v)
			t.Log("encoded len", enc.Len())

			chk3 := ChunkRef{}
			dec := encoding.DecWith(enc.Get())
			chk3.Decode(&dec, v)

			require.Equal(t, chk2, chk3)
		})

		t.Run("delta", func(t *testing.T) {
			enc := encoding.EncWith(make([]byte, 0, 128))
			chk2.EncodeDelta(&enc, v, &chk1)
			t.Log("encoded len", enc.Len())

			chk3 := ChunkRef{}
			dec := encoding.DecWith(enc.Get())
			chk3.DecodeDelta(&dec, v, &chk1)

			require.Equal(t, chk2, chk3)
		})
	})

	t.Run("Index", func(t *testing.T) {
		orig := Index{
			Fingerprint: model.Fingerprint(0xafbfcfdf),
			Chunks: []ChunkRef{
				ChunkRef{From: 0, Through: 1000, Checksum: 123},
				ChunkRef{From: 500, Through: 1500, Checksum: 234},
			},
			Offsets: []Offset{
				Offset{Offset: 0, Len: 1024},
				Offset{Offset: 1025, Len: 1024},
			},
			Fields: shared.NewSetFromLiteral("field_a", "field_b", "field_c"),
		}

		t.Run("default", func(t *testing.T) {
			enc := encoding.EncWith(make([]byte, 0, 128))
			orig.Encode(&enc, v)
			t.Log("encoded len", enc.Len())

			decoded := Index{}
			dec := encoding.DecWith(enc.Get())
			decoded.Decode(&dec, v)

			require.Equal(t, orig, decoded)
		})
	})

	t.Run("Footer", func(t *testing.T) {
		orig := Footer{
			IndexOffset: 512,
			IndexLen:    1024,
			Checksum:    123456,
		}

		t.Run("default", func(t *testing.T) {
			enc := encoding.EncWith(make([]byte, 0, 128))
			orig.Encode(&enc, v)
			t.Log("encoded len", enc.Len())

			decoded := Footer{}
			dec := encoding.DecWith(enc.Get())
			decoded.Decode(&dec, v)

			require.Equal(t, orig, decoded)
		})
	})

	t.Run("Block", func(t *testing.T) {

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

		t.Run("default", func(t *testing.T) {
			enc := encoding.EncWith(make([]byte, 0, 4<<10)) // 4K buffer
			blk.Encode(&enc, v)
			t.Log("encoded len", enc.Len())

			decoded := Block{}
			dec := encoding.DecWith(enc.Get())
			err := decoded.Decode(&dec, v)
			require.NoError(t, err)

			require.Equal(t, blk, decoded)
		})
	})
}
