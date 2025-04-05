package sarama

import "os"

type RecordBatchReader interface {
}

type byteRecordBatchReader struct {
	raw []byte
	off int
}

type fileRecordBatchReader struct {
	// file is the underlying file handle
	file *os.File
}

func NewRecordBatchReader(raw []byte, off int) RecordBatchReader {
	return &byteRecordBatchReader{raw: raw, off: off}
}

func (r *byteRecordBatchReader) Next() (*RecordBatch, error) {
	if r.off >= len(r.raw) {
		return nil, nil
	}

	batch := &RecordBatch{}
	handle := &realDecoder{raw: r.raw, off: r.off, stack: make([]pushDecoder, 0)}
	if err := batch.decode(handle); err != nil {
		return nil, err
	}

	r.off += batch.rawBatchLength
	return batch, nil
}

// DecodeRecordBatch decodes a RecordBatch from the given raw byte slice starting at the specified offset.
func DecodeRecordBatch(raw []byte, offset int) (*RecordBatch, error) {
	handle := &realDecoder{raw: raw, off: offset, stack: make([]pushDecoder, 0)}
	batch := &RecordBatch{}
	if err := batch.decode(handle); err != nil {
		return nil, err
	}
	return batch, nil
}
