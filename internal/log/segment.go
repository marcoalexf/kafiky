package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type Segment struct {
	mu         sync.RWMutex
	index      *os.File
	store      *os.File
	baseOffset uint64
	nextOffset uint64
}

func NewSegment(baseOffset uint64) *Segment {
	storeFileName := fmt.Sprintf("%d.store", baseOffset)
	indexFileName := fmt.Sprintf("%d.index", baseOffset)

	fStore, err := os.OpenFile(storeFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open store file: %v", err))
	}

	fIndex, err := os.OpenFile(indexFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open index file: %v", err))
	}

	// Rebuild the index from the store file on startup to keep them consistent
	if err := RebuildIndex(storeFileName); err != nil {
		panic(fmt.Sprintf("Failed to rebuild index for %s: %v", storeFileName, err))
	}

	segment := &Segment{
		baseOffset: baseOffset,
		store:      fStore,
		index:      fIndex,
	}

	// Set nextOffset based on how many records exist in the index file
	info, err := fIndex.Stat()
	if err != nil {
		panic(fmt.Sprintf("Failed to stat index file: %v", err))
	}

	// Each index entry is 12 bytes (4 bytes offset + 8 bytes position)
	entries := info.Size() / 12
	segment.nextOffset = baseOffset + uint64(entries)

	return segment
}

func (s *Segment) Append(data []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset, err := s.store.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	lengthBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lengthBuf, uint64(len(data)))

	if _, err := s.store.Write(lengthBuf); err != nil {
		return 0, err
	}

	if _, err := s.store.Write(data); err != nil {
		return 0, err
	}

	relativeOffset := s.nextOffset - s.baseOffset
	indexBuf := make([]byte, 12)
	binary.BigEndian.PutUint32(indexBuf[:4], uint32(relativeOffset))
	binary.BigEndian.PutUint64(indexBuf[4:], uint64(offset))

	absoluteOffset := s.nextOffset
	s.nextOffset++

	if _, err := s.index.Write(indexBuf); err != nil {
		return 0, err
	}

	return absoluteOffset, nil
}

func (s *Segment) Read(offset uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	relativeOffset := offset - s.baseOffset

	if _, err := s.index.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}

	entry := make([]byte, 12)
	for {
		_, err := io.ReadFull(s.index, entry)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			panic(err)
		}

		rel := binary.BigEndian.Uint32(entry[:4])
		pos := binary.BigEndian.Uint64(entry[4:])

		if uint64(rel) == relativeOffset {
			if _, err := s.store.Seek(int64(pos), io.SeekStart); err != nil {
				panic(err)
			}

			dataLength := make([]byte, 8)
			if _, err := io.ReadFull(s.store, dataLength); err != nil {
				panic(err)
			}

			recordLen := binary.BigEndian.Uint64(dataLength)
			slice := make([]byte, recordLen)

			if _, err := io.ReadFull(s.store, slice); err != nil {
				panic(err)
			}

			return slice, nil
		}
	}

	return nil, errors.New("Offset not found")
}
