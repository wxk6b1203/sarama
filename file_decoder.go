package sarama

import (
	"encoding/binary"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"io"
	"math"
	"os"
)

type fileDecoder struct {
	file     *os.File
	stack    []pushDecoder
	registry metrics.Registry
}

func NewFileDecoder(pathname string, direction bool, registry metrics.Registry) (packetDecoder, error) {
	file, err := OpenFile(pathname, direction, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &fileDecoder{
		file:     file,
		stack:    make([]pushDecoder, 0),
		registry: registry,
	}, nil
}

func (fd *fileDecoder) seek(pos int64) (int64, error) {
	if off, err := fd.file.Seek(pos, io.SeekStart); err != nil {
		return 0, err
	} else {
		return off, nil
	}
}

// withPosSaver 保存当前文件位置，执行函数，如果出错则恢复到原来位置
func withPosSaver[T any](fd *fileDecoder, fn func() (T, error)) (T, error) {
	var defaultT T
	pos, err := fd.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return defaultT, err
	}

	result, err := fn()
	if err != nil {
		_, seekErr := fd.seek(pos)
		if seekErr != nil {
			// 如果恢复位置也失败，记录到原始错误中
			return defaultT, fmt.Errorf("original error: %w, failed to restore position: %v", err, seekErr)
		}
	}
	return result, err
}

func (fd *fileDecoder) getInt8() (int8, error) {
	return withPosSaver(fd, func() (int8, error) {
		return fd.getInt8Raw()
	})
}

func (fd *fileDecoder) getInt8Raw() (int8, error) {
	buf := make([]byte, 1)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return 0, err
	}
	if cnt < 1 {
		return -1, ErrInsufficientData
	}
	return int8(buf[0]), nil
}

func (fd *fileDecoder) getInt16() (int16, error) {
	return withPosSaver(fd, func() (int16, error) {
		return fd.getInt16Raw()
	})
}

func (fd *fileDecoder) getInt16Raw() (int16, error) {
	buf := make([]byte, 2)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return 0, err
	}
	if cnt < 2 {
		return -1, ErrInsufficientData
	}
	tmp := int16(binary.BigEndian.Uint16(buf))
	return tmp, nil
}

func (fd *fileDecoder) getInt32() (int32, error) {
	return withPosSaver(fd, func() (int32, error) {
		return fd.getInt32Raw()
	})
}

func (fd *fileDecoder) getInt32Raw() (int32, error) {
	buf := make([]byte, 4)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return 0, err
	}
	if cnt < 4 {
		return -1, ErrInsufficientData
	}

	tmp := int32(binary.BigEndian.Uint32(buf))
	return tmp, nil
}

func (fd *fileDecoder) getInt64() (int64, error) {
	return withPosSaver(fd, func() (int64, error) {
		return fd.getInt64Raw()
	})
}

func (fd *fileDecoder) getInt64Raw() (int64, error) {
	buf := make([]byte, 8)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return 0, err
	}
	if cnt < 8 {
		return -1, ErrInsufficientData
	}

	tmp := int64(binary.BigEndian.Uint64(buf))
	return tmp, nil
}

func (fd *fileDecoder) getVarint() (int64, error) {
	return withPosSaver(fd, func() (int64, error) {
		return fd.getVarintRaw()
	})
}

func (fd *fileDecoder) getVarintRaw() (int64, error) {
	buf := make([]byte, 10)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return 0, err
	}
	if cnt < binary.MaxVarintLen16 {
		return -1, ErrInsufficientData
	}
	tmp, n := binary.Varint(buf)
	if n == 0 {
		return -1, ErrInsufficientData
	}
	if n < 0 {
		return -1, errVarintOverflow
	}

	return tmp, nil
}

func (fd *fileDecoder) getUVarint() (uint64, error) {
	return withPosSaver(fd, func() (uint64, error) {
		return fd.getUVarintRaw()
	})
}

func (fd *fileDecoder) getUVarintRaw() (uint64, error) {
	buf := make([]byte, 10)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return 0, err
	}
	if cnt < binary.MaxVarintLen16 {
		return 0, ErrInsufficientData
	}
	tmp, n := binary.Uvarint(buf)
	if n == 0 {
		return 0, ErrInsufficientData
	}
	if n < 0 {
		return 0, errVarintOverflow
	}

	return tmp, nil
}

func (fd *fileDecoder) getFloat64() (float64, error) {
	return withPosSaver(fd, func() (float64, error) {
		return fd.getFloat64Raw()
	})
}

func (fd *fileDecoder) getFloat64Raw() (float64, error) {
	buf := make([]byte, 8)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return 0, err
	}
	if cnt < 8 {
		return -1, ErrInsufficientData
	}
	tmp := math.Float64frombits(binary.BigEndian.Uint64(buf))
	return tmp, nil
}

func (fd *fileDecoder) getArrayLength() (int, error) {
	return withPosSaver(fd, func() (int, error) {
		return fd.getArrayLengthRaw()
	})
}

// TODO: validate array length
func (fd *fileDecoder) getArrayLengthRaw() (int, error) {
	buf := make([]byte, 4)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return -1, err
	}
	if cnt < 4 {
		return -1, ErrInsufficientData
	}

	tmp := int(int32(binary.BigEndian.Uint32(buf)))
	if tmp > int(MaxResponseSize) {
		return -1, errInvalidArrayLength
	}
	return tmp, nil
}

func (fd *fileDecoder) getCompactArrayLength() (int, error) {
	n, err := fd.getUVarint()
	if err != nil {
		return 0, err
	}

	if n == 0 {
		return 0, nil
	}

	return int(n) - 1, nil
}

func (fd *fileDecoder) getBool() (bool, error) {
	return withPosSaver(fd, func() (bool, error) {
		return fd.getBoolRaw()
	})
}

func (fd *fileDecoder) getBoolRaw() (bool, error) {
	b, err := fd.getInt8()
	if err != nil || b == 0 {
		return false, err
	}
	if b != 1 {
		return false, errInvalidBool
	}
	return true, nil
}

func (fd *fileDecoder) getEmptyTaggedFieldArray() (int, error) {
	return withPosSaver(fd, func() (int, error) {
		return fd.getEmptyTaggedFieldArrayRaw()
	})
}

func (fd *fileDecoder) getEmptyTaggedFieldArrayRaw() (int, error) {
	tagCount, err := fd.getUVarint()
	if err != nil {
		return 0, err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for i := uint64(0); i < tagCount; i++ {
		// fetch and ignore tag identifier
		_, err := fd.getUVarint()
		if err != nil {
			return 0, err
		}
		length, err := fd.getUVarint()
		if err != nil {
			return 0, err
		}
		if _, err := fd.getRawBytes(int(length)); err != nil {
			return 0, err
		}
	}

	return 0, nil
}

// collections

func (fd *fileDecoder) getBytes() ([]byte, error) {
	return withPosSaver(fd, func() (bytes []byte, err error) {
		return fd.getBytesRaw()
	})
}

func (fd *fileDecoder) getBytesRaw() ([]byte, error) {
	tmp, err := fd.getInt32()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return fd.getRawBytes(int(tmp))
}

func (fd *fileDecoder) getVarintBytes() ([]byte, error) {
	return withPosSaver(fd, func() (bytes []byte, err error) {
		return fd.getVarintBytesRaw()
	})
}

func (fd *fileDecoder) getVarintBytesRaw() ([]byte, error) {
	tmp, err := fd.getVarint()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return fd.getRawBytes(int(tmp))
}

func (fd *fileDecoder) getCompactBytes() ([]byte, error) {
	return withPosSaver(fd, func() (bytes []byte, err error) {
		return fd.getCompactBytesRaw()
	})
}

func (fd *fileDecoder) getCompactBytesRaw() ([]byte, error) {
	n, err := fd.getUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)
	return fd.getRawBytes(length)
}

func (fd *fileDecoder) getStringLength() (int, error) {
	length, err := fd.getInt16()
	if err != nil {
		return 0, err
	}

	n := int(length)

	switch {
	case n < -1:
		return 0, errInvalidStringLength
	case n > fd.remaining():
		return 0, ErrInsufficientData
	}

	return n, nil
}

func (fd *fileDecoder) getString() (string, error) {
	return withPosSaver(fd, func() (string, error) {
		return fd.getStringRaw()
	})
}

func (fd *fileDecoder) getStringRaw() (string, error) {
	n, err := fd.getStringLength()
	if err != nil || n == -1 {
		return "", err
	}

	buf := make([]byte, n)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return "", err
	}
	if cnt < n {
		return "", ErrInsufficientData
	}
	tmpStr := string(buf)
	return tmpStr, nil
}

func (fd *fileDecoder) getNullableString() (*string, error) {
	return withPosSaver(fd, func() (*string, error) {
		return fd.getNullableStringRaw()
	})
}

func (fd *fileDecoder) getNullableStringRaw() (*string, error) {
	n, err := fd.getStringLength()
	if err != nil || n == -1 {
		return nil, err
	}

	buf := make([]byte, n)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return nil, err
	}

	if cnt < n {
		return nil, ErrInsufficientData
	}
	tmpStr := string(buf)

	return &tmpStr, nil
}

func (fd *fileDecoder) getCompactString() (string, error) {
	return withPosSaver(fd, func() (string, error) {
		return fd.getCompactStringRaw()
	})
}

func (fd *fileDecoder) getCompactStringRaw() (string, error) {
	n, err := fd.getUVarint()
	if err != nil {
		return "", err
	}

	length := int(n - 1)
	if length < 0 {
		return "", errInvalidByteSliceLength
	}

	buf := make([]byte, length)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return "", err
	}
	if cnt < length {
		return "", ErrInsufficientData
	}
	tmpStr := string(buf)
	return tmpStr, nil
}

func (fd *fileDecoder) getCompactNullableString() (*string, error) {
	return withPosSaver(fd, func() (*string, error) {
		return fd.getCompactNullableStringRaw()
	})
}

func (fd *fileDecoder) getCompactNullableStringRaw() (*string, error) {
	n, err := fd.getUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)

	if length < 0 {
		return nil, err
	}

	buf := make([]byte, length)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return nil, err
	}
	if cnt < length {
		return nil, ErrInsufficientData
	}

	tmpStr := string(buf)
	return &tmpStr, nil
}

func (fd *fileDecoder) getCompactInt32Array() ([]int32, error) {
	return withPosSaver(fd, func() ([]int32, error) {
		return fd.getCompactInt32ArrayRaw()
	})
}

func (fd *fileDecoder) getCompactInt32ArrayRaw() ([]int32, error) {
	n, err := fd.getUVarint()
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	arrayLength := int(n) - 1

	ret := make([]int32, arrayLength)

	for i := range ret {
		tmp, err := fd.getInt32()
		if err != nil {
			return nil, err
		}
		ret[i] = tmp
	}

	return ret, nil
}

func (fd *fileDecoder) getInt32Array() ([]int32, error) {
	return withPosSaver(fd, func() ([]int32, error) {
		return fd.getInt32ArrayRaw()
	})
}

func (fd *fileDecoder) getInt32ArrayRaw() ([]int32, error) {
	cnt, err := fd.getInt32()
	if err != nil {
		return nil, err
	}

	// TODO: validate array length

	if cnt == 0 {
		return nil, nil
	}

	if cnt < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int32, cnt)
	for i := range ret {
		tmp, err := fd.getInt32()
		if err != nil {
			return nil, err
		}
		ret[i] = tmp
	}

	return ret, nil
}

func (fd *fileDecoder) getInt64Array() ([]int64, error) {
	return withPosSaver(fd, func() ([]int64, error) {
		return fd.getInt64ArrayRaw()
	})
}

func (fd *fileDecoder) getInt64ArrayRaw() ([]int64, error) {
	cnt, err := fd.getInt32()
	if err != nil {
		return nil, err
	}

	// TODO: validate array length

	if cnt == 0 {
		return nil, nil
	}
	if cnt < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int64, cnt)

	for i := range ret {
		tmp, err := fd.getInt64()
		if err != nil {
			return nil, err
		}
		ret[i] = tmp
	}

	return ret, nil
}

func (fd *fileDecoder) getStringArray() ([]string, error) {
	return withPosSaver(fd, func() ([]string, error) {
		return fd.getStringArrayRaw()
	})
}

func (fd *fileDecoder) getStringArrayRaw() ([]string, error) {
	cnt, err := fd.getInt32()
	if err != nil {
		return nil, err
	}

	if cnt == 0 {
		return nil, nil
	}

	if cnt < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]string, cnt)
	for i := range ret {
		tmp, err := fd.getString()
		if err != nil {
			return nil, err
		}
		ret[i] = tmp
	}

	return ret, nil
}

// subsets

// FIXME: cache file info and update it periodically
func (fd *fileDecoder) remaining() int {
	fileInfo, err := os.Stat(fd.file.Name())
	if err != nil {
		return -1
	}
	fileSize := fileInfo.Size()
	currentOffset, err := fd.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1
	}
	remainingBytes := fileSize - currentOffset
	return int(remainingBytes)
}

func (fd *fileDecoder) getSubset(length int) (packetDecoder, error) {
	// TODO
	return fd, nil
}

func (fd *fileDecoder) getRawBytes(length int) ([]byte, error) {
	return withPosSaver(fd, func() ([]byte, error) {
		return fd.getRawBytesRaw(length)
	})
}

func (fd *fileDecoder) getRawBytesRaw(length int) ([]byte, error) {
	buf := make([]byte, length)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return nil, err
	}
	if cnt < length {
		return nil, ErrInsufficientData
	}
	return buf, nil
}

func (fd *fileDecoder) peek(offset, length int) (packetDecoder, error) {
	// TODO
	return fd, nil
}

func (fd *fileDecoder) peekInt8(offset int) (int8, error) {
	// FIXME: implement it
	buf := make([]byte, 1)
	cnt, err := fd.file.Read(buf)
	if err != nil {
		return -1, err
	}
	if cnt < 1 {
		return -1, ErrInsufficientData
	}
	return int8(buf[0]), nil
}

// stacks

// TODO
func (fd *fileDecoder) push(in pushDecoder) error {
	if in == nil {
		return nil
	}
	in.saveOffset(fd.remaining())
	fd.stack = append(fd.stack, in)
	return nil
}

// TODO
func (fd *fileDecoder) pop() error {
	return nil
}

func (fd *fileDecoder) metricRegistry() metrics.Registry {
	return fd.registry
}
