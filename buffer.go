package wavelet

import (
	"github.com/djherbis/buffer"
)

type fileBufferPool struct {
	fileSize int64
	filePool buffer.Pool
}

func newFileBufferPool(fileSize int64, dir string) *fileBufferPool {
	filePool := buffer.NewFilePool(fileSize, dir)
	return &fileBufferPool{
		fileSize: fileSize,
		filePool: filePool,
	}
}

// GetUnbounded returns an unbounded buffer.
func (b *fileBufferPool) GetUnbounded() buffer.Buffer {
	return buffer.NewPartition(b.filePool)
}

// GetBounded creates a bounded buffer of the given size.
func (b *fileBufferPool) GetBounded(size int64) (buffer.BufferAt, error) {
	buffers := []buffer.BufferAt{}

	filesCount := size / b.fileSize
	if size%b.fileSize > 0 {
		filesCount++
	}

	cleanup := func() {
		for _, f := range buffers {
			b.filePool.Put(f)
		}
	}

	for i := 0; i < int(filesCount); i++ {
		file, err := b.filePool.Get()
		if err != nil {
			cleanup()
			return nil, err
		}

		buffers = append(buffers, file.(buffer.BufferAt))
	}

	return &boundedFileBuffer{Files: buffers, BufferAt: buffer.NewMultiAt(buffers...)}, nil
}

func (b *fileBufferPool) Put(buf buffer.Buffer) {
	buffer, ok := buf.(*boundedFileBuffer)
	if ok {
		for _, f := range buffer.Files {
			b.filePool.Put(f)
		}
	}

	buf.Reset()
}

type boundedFileBuffer struct {
	Files []buffer.BufferAt
	buffer.BufferAt
}
