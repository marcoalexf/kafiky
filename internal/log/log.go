package log

import (
	"fmt"
	"slices"
	"sync"
)

type Log struct {
	mu       sync.RWMutex
	segments []*Segment
}

func NewLog() *Log {
	files, err := ListFilesWithExtension(".", ".store")
	if err != nil {
		panic(fmt.Errorf("failed to list store files: %w", err))
	}

	if len(files) == 0 {
		seg := NewSegment(0)
		return &Log{segments: []*Segment{seg}}
	}

	baseOffsets := make([]uint64, 0, len(files))
	for _, f := range files {
		base, err := extractBaseOffset(f)
		if err != nil {
			panic(fmt.Errorf("invalid segment filename %q: %w", f, err))
		}
		baseOffsets = append(baseOffsets, base)
	}

	slices.Sort(baseOffsets)

	var segments []*Segment
	for _, base := range baseOffsets {
		storeFile := fmt.Sprintf("%d.store", base)
		if err := RebuildIndex(storeFile); err != nil {
			panic(fmt.Errorf("failed to rebuild index for %s: %w", storeFile, err))
		}
		segment := NewSegment(base)
		segments = append(segments, segment)
	}

	return &Log{segments: segments}
}

func (l *Log) Append(data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	activeSegment := l.segments[len(l.segments)-1]

	offset, err := activeSegment.Append(data)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, segment := range l.segments {
		if offset >= segment.baseOffset && offset < segment.nextOffset {
			return segment.Read(offset)
		}
	}

	return nil, fmt.Errorf("offset %d not found in any segment", offset)
}
