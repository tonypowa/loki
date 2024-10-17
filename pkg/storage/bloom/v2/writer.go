package bloom

import (
	"io"
)

type WriteSeekCloser interface {
	io.Writer
	io.Seeker
	io.Closer
}

func NewInMemoryEncoder() {}

func NewFileEncoder() {}

type BlockEncoder struct {
	// memory or file backed reader
	writer WriteSeekCloser
}
