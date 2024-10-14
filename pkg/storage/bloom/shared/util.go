package shared

import (
	"hash"
	"hash/crc32"
	"sync"
)

var (
	CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)

	// Pool of crc32 hash
	Crc32HashPool = ChecksumPool{
		Pool: sync.Pool{
			New: func() interface{} {
				return crc32.New(CastagnoliTable)
			},
		},
	}
)

type ChecksumPool struct {
	sync.Pool
}

func (p *ChecksumPool) Get() hash.Hash32 {
	h := p.Pool.Get().(hash.Hash32)
	h.Reset()
	return h
}

func (p *ChecksumPool) Put(h hash.Hash32) {
	p.Pool.Put(h)
}

type Set[V comparable] struct {
	internal map[V]struct{}
}

func NewSet[V comparable](size int) Set[V] {
	return Set[V]{make(map[V]struct{}, size)}
}

func NewSetFromLiteral[V comparable](v ...V) Set[V] {
	set := NewSet[V](len(v))
	for _, elem := range v {
		set.Add(elem)
	}
	return set
}

func (s Set[V]) Add(v V) bool {
	_, ok := s.internal[v]
	if !ok {
		s.internal[v] = struct{}{}
	}
	return !ok
}

func (s Set[V]) Len() int {
	return len(s.internal)
}

func (s Set[V]) Items() []V {
	set := make([]V, 0, s.Len())
	for k := range s.internal {
		set = append(set, k)
	}
	return set
}

func (s Set[V]) Union(other Set[V]) {
	for _, v := range other.Items() {
		s.Add(v)
	}
}
