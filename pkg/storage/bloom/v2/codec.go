package bloom

import "github.com/grafana/loki/v3/pkg/util/encoding"

type Serializable[T any] interface {
	Encode(enc *encoding.Encbuf, v Version)
	Decode(dec *encoding.Decbuf, v Version) error
}

type DeltaSerializable[T any] interface {
	EncodeDelta(enc *encoding.Encbuf, v Version, prev T)
	DecodeDelta(dec *encoding.Decbuf, v Version, prev T) error
}
