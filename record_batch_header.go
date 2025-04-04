package sarama

import "fmt"

type RecordBatchHeader struct {
	RecordBatch
	RecordLen  int32
	RecordSize int
}

func decodeRecordHeader(pd packetDecoder) (*RecordBatchHeader, error) {
	header := &RecordBatchHeader{}
	if err := header.decode(pd); err != nil {
		return nil, err
	}

	return header, nil
}

// DecodeBatchHeader decodes the RecordBatchHeader from the given raw byte slice.
func DecodeBatchHeader(raw []byte, offset int) (*RecordBatchHeader, error) {
	handle := &realDecoder{raw: raw, off: offset, stack: make([]pushDecoder, 0)}
	return decodeRecordHeader(handle)
}

// encode encodes the RecordBatchHeader into the given packetEncoder.
// Note that this method does not include the CRC32 checksum and
// length field for the records.
// The CRC32 checksum and length field are calculated separately
func (b *RecordBatchHeader) encode(pe packetEncoder) error {
	if b.Version != 2 {
		return PacketEncodingError{fmt.Sprintf("unsupported record batch version (%d)", b.Version)}
	}
	pe.putInt64(b.FirstOffset)
	// push a length field with header length only
	pe.putInt32(recordBatchOverhead)
	pe.putInt32(b.PartitionLeaderEpoch)
	pe.putInt8(b.Version)
	// push a CRC32 field with a placeholder for the length
	pe.putInt32(0)
	pe.putInt16(b.computeAttributes())
	pe.putInt32(b.LastOffsetDelta)

	if err := (Timestamp{&b.FirstTimestamp}).encode(pe); err != nil {
		return err
	}

	if err := (Timestamp{&b.MaxTimestamp}).encode(pe); err != nil {
		return err
	}

	pe.putInt64(b.ProducerID)
	pe.putInt16(b.ProducerEpoch)
	pe.putInt32(b.FirstSequence)

	if err := pe.putArrayLength(len(b.Records)); err != nil {
		return err
	}

	return nil
}

// decode decodes the RecordBatchHeader from the given packetDecoder.
// Note that this method does not include the CRC32 checksum and
// length field for the records.
func (b *RecordBatchHeader) decode(pd packetDecoder) (err error) {
	if b.FirstOffset, err = pd.getInt64(); err != nil {
		return err
	}

	batchLen, err := pd.getInt32()
	if err != nil {
		return err
	}

	b.RecordLen = batchLen - recordBatchOverhead

	if b.PartitionLeaderEpoch, err = pd.getInt32(); err != nil {
		return err
	}

	if b.Version, err = pd.getInt8(); err != nil {
		return err
	}

	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	attributes, err := pd.getInt16()
	if err != nil {
		return err
	}
	b.Codec = CompressionCodec(int8(attributes) & compressionCodecMask)
	b.Control = attributes&controlMask == controlMask
	b.LogAppendTime = attributes&timestampTypeMask == timestampTypeMask
	b.IsTransactional = attributes&isTransactionalMask == isTransactionalMask

	if b.LastOffsetDelta, err = pd.getInt32(); err != nil {
		return err
	}

	if err = (Timestamp{&b.FirstTimestamp}).decode(pd); err != nil {
		return err
	}

	if err = (Timestamp{&b.MaxTimestamp}).decode(pd); err != nil {
		return err
	}

	if b.ProducerID, err = pd.getInt64(); err != nil {
		return err
	}

	if b.ProducerEpoch, err = pd.getInt16(); err != nil {
		return err
	}

	if b.FirstSequence, err = pd.getInt32(); err != nil {
		return err
	}

	numRecs, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	b.RecordSize = numRecs

	return nil
}
