package bloom

import (
	"bytes"
	"fmt"
	"io"
	"os"

	v2 "github.com/grafana/loki/v3/pkg/iter/v2"
	"github.com/grafana/loki/v3/pkg/storage/bloom/shared"
	"github.com/grafana/loki/v3/pkg/util/encoding"
	"github.com/pkg/errors"
)

type NoopCloser struct {
	*bytes.Reader
}

func (NoopCloser) Close() error {
	return nil
}

func NewInMemoryDecoder(data []byte) (*BlockDecoder, error) {
	r := bytes.NewReader(data)
	return &BlockDecoder{reader: &NoopCloser{r}}, nil
}

func NewFileDecoder(path string) (*BlockDecoder, error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &BlockDecoder{reader: fp}, nil
}

var _ v2.Iterator[*shared.Bloom] = &BlockDecoder{}

type BlockDecoder struct {
	// memory or file backed reader
	reader io.ReadSeekCloser
	// sections that are loaded into memory
	header      Header
	index       Index
	footer      Footer
	initialized bool
	// fields for lazy bloom decoding
	idx  int
	curr *shared.Bloom
	err  error
}

func (r *BlockDecoder) Close() error {
	return r.reader.Close()
}

func (r *BlockDecoder) Footer() (Footer, error) {
	if err := r.ensureInit(); err != nil {
		return Footer{}, err
	}
	return r.footer, nil
}

func (r *BlockDecoder) Header() (Header, error) {
	if err := r.ensureInit(); err != nil {
		return Header{}, err
	}
	return r.header, nil
}

func (r *BlockDecoder) Index() (Index, error) {
	if err := r.ensureInit(); err != nil {
		return Index{}, err
	}
	return r.index, nil
}

func (r *BlockDecoder) Err() error {
	return r.err
}

func (r *BlockDecoder) Next() bool {
	err := r.ensureInit()
	if err != nil {
		r.err = err
		return false
	}

	if r.idx+1 < len(r.index.Offsets) {
		r.idx++
		r.curr = nil
		return true
	}

	return false
}

func (r *BlockDecoder) At() *shared.Bloom {
	err := r.ensureInit()
	if err != nil {
		r.err = err
		return nil
	}

	if r.curr != nil {
		return r.curr
	}

	offset := r.index.Offsets[r.idx]
	if _, err := r.reader.Seek(int64(offset.Offset), io.SeekStart); err != nil {
		r.err = err
		return nil
	}

	// TODO: Use pool
	buf := make([]byte, offset.Len)
	if _, err := r.reader.Read(buf); err != nil {
		r.err = err
		return nil
	}
	dec := encoding.DecWith(buf)

	r.curr = &shared.Bloom{}
	r.curr.Decode(&dec)

	return r.curr
}

func (r *BlockDecoder) ensureInit() error {
	if r.initialized {
		return nil
	}
	return r.init()
}

func (r *BlockDecoder) init() error {
	// TODO: Use buffer from pool
	buf := make([]byte, 4<<10) // 4k buffer

	// 1. Read magic number and header
	n, err := r.reader.Read(buf[0:4]) // 4 bytes
	if err != nil || n < 1 {
		return err
	}
	dec := encoding.DecWith(buf)
	sig := dec.Be32()
	if sig != MagicNumber {
		return fmt.Errorf("invalid magic number: got %x, expected %x", sig, MagicNumber)
	}

	n, err = r.reader.Read(buf[0:2]) // 2 bytes
	if err != nil || n < 2 {
		return errors.Wrap(err, "init block decoder")
	}
	dec = encoding.DecWith(buf)
	r.header.Decode(&dec, 0)

	// 2. Seek to and read footer
	// last 20 bytes are (byteOffset: 8 byte u64, bytesLen: 8 byte u64, checksum: 4 byte u32)
	if _, err := r.reader.Seek(-20, io.SeekEnd); err != nil {
		return err
	}

	n, err = r.reader.Read(buf[0:20]) // 20 bytes
	if err != nil || n < 20 {
		return errors.Wrap(err, "init block decoder")
	}
	dec = encoding.DecWith(buf)
	r.footer.Decode(&dec, r.header.Version)

	// 3. Seek to and read header
	if _, err := r.reader.Seek(int64(r.footer.IndexOffset), io.SeekStart); err != nil {
		return err
	}

	buf = make([]byte, r.footer.IndexLen)
	n, err = r.reader.Read(buf)
	if err != nil || n < int(r.footer.IndexLen) {
		return errors.Wrap(err, "init block decoder")
	}
	dec = encoding.DecWith(buf)
	r.index.Decode(&dec, r.header.Version)

	r.idx = -1
	r.initialized = true
	return nil
}
