package workspacearchive

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
)

var errZIPProfile = errors.New("unsupported project ZIP layout (ZIP64, self-extracting, split or trailing data)")

// boundDirectory validates the complete central directory in constant space
// before archive/zip allocates one File object for every declared entry. Counts
// are checked both in the end record and against actual directory headers.
// This project archive profile excludes ZIP64 and self-extracting containers.
func boundDirectory(ctx context.Context, source io.ReaderAt, size int64, limits Limits) error {
	if size < 22 {
		return errZIPProfile
	}
	tailSize := min(size, int64(22+65535))
	tail := make([]byte, int(tailSize))
	if _, err := source.ReadAt(tail, size-tailSize); err != nil {
		return err
	}
	end := -1
	for i := len(tail) - 22; i >= 0; i-- {
		if binary.LittleEndian.Uint32(tail[i:]) == 0x06054b50 && i+22+int(binary.LittleEndian.Uint16(tail[i+20:])) == len(tail) {
			end = i
			break
		}
	}
	if end < 0 {
		return errZIPProfile
	}
	record := tail[end:]
	disk := binary.LittleEndian.Uint16(record[4:])
	directoryDisk := binary.LittleEndian.Uint16(record[6:])
	diskCount := binary.LittleEndian.Uint16(record[8:])
	count := binary.LittleEndian.Uint16(record[10:])
	directorySize := int64(binary.LittleEndian.Uint32(record[12:]))
	offset := int64(binary.LittleEndian.Uint32(record[16:]))
	if disk != 0 || directoryDisk != 0 || diskCount != count || count == 0xffff || directorySize == 0xffffffff || offset == 0xffffffff {
		return errZIPProfile
	}
	if int(count) > limits.Entries {
		return errors.New("archive entry limit exceeded")
	}
	endOffset := size - tailSize + int64(end)
	if offset+directorySize != endOffset {
		return errZIPProfile
	}
	if count == 0 {
		if offset != 0 || directorySize != 0 {
			return errZIPProfile
		}
		return nil
	}
	var first [4]byte
	if _, err := source.ReadAt(first[:], 0); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(first[:]) != 0x04034b50 {
		return errZIPProfile
	}
	var header [46]byte
	actual := 0
	for position := offset; position < endOffset; {
		if err := ctx.Err(); err != nil {
			return err
		}
		actual++
		if actual > limits.Entries {
			return errors.New("archive entry limit exceeded")
		}
		if endOffset-position < int64(len(header)) {
			return errZIPProfile
		}
		if _, err := source.ReadAt(header[:], position); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(header[:]) != 0x02014b50 {
			return errZIPProfile
		}
		name := int64(binary.LittleEndian.Uint16(header[28:]))
		extra := int64(binary.LittleEndian.Uint16(header[30:]))
		comment := int64(binary.LittleEndian.Uint16(header[32:]))
		if name == 0 || name-1 > int64(limits.PathBytes) {
			return errors.New("archive path length exceeded")
		}
		if binary.LittleEndian.Uint16(header[34:]) != 0 || binary.LittleEndian.Uint32(header[20:]) == 0xffffffff || binary.LittleEndian.Uint32(header[24:]) == 0xffffffff || binary.LittleEndian.Uint32(header[42:]) == 0xffffffff {
			return errZIPProfile
		}
		if int64(binary.LittleEndian.Uint32(header[42:])) >= offset {
			return errZIPProfile
		}
		length := int64(len(header)) + name + extra + comment
		if length > endOffset-position {
			return errZIPProfile
		}
		position += length
	}
	if actual != int(count) {
		return errZIPProfile
	}
	return nil
}
