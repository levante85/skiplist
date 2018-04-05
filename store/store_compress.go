package store

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

// Compress is the interface compression algorithms should implement to be
// used inside the store, since the store in a fixed size store, meaning each
// record should have fixed size, the algos implementing this interface should
// make sure that the compressed size is always the same if 100 bytes in, output
// should alwasy be 10byte out
type Compress interface {
	Decode([]byte) (int, error)
	Encode([]byte) (int, error)
	Close() (error, error)
}

// Gzip compression implementation
type Gzip struct {
	bufR   *bytes.Buffer
	bufW   *bytes.Buffer
	reader *gzip.Reader
	writer *gzip.Writer
}

// NewGzip creates a new gzip implementation
func NewGzip() *Gzip {
	return &Gzip{
		&bytes.Buffer{},
		&bytes.Buffer{},
		nil,
		nil,
	}
}

// Encode does what is says
func (g *Gzip) Encode(in []byte) ([]byte, error) {
	if g.writer == nil {
		g.writer = gzip.NewWriter(g.bufW)
	}

	if g.bufW.Len() > 0 {
		g.bufW.Reset()
	}

	_, err := g.writer.Write(in)
	if err != nil {
		return nil, err
	}

	return g.bufW.Bytes(), nil

}

// Decode does what is says
func (g *Gzip) Decode(in []byte) ([]byte, error) {
	if g.reader == nil {
		var err error
		g.reader, err = gzip.NewReader(g.bufR)
		if err != nil {
			return nil, err
		}
	}

	if g.bufR.Len() > 0 {
		g.bufR.Reset()
	}

	_, err := g.bufR.Write(in)
	if err != nil {
		return nil, err
	}

	out, err := ioutil.ReadAll(g.reader)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Close the Gzip Compressor and the underlying reader and writer
func (g *Gzip) Close() (errR error, errW error) {
	if g.reader != nil {
		errR = g.reader.Close()
	}

	if g.writer != nil {
		errW = g.writer.Close()
	}

	return

}
