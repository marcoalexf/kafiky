package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func ListFilesWithExtension(dir string, ext string) ([]string, error) {
	var files []string

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ext) {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

func RebuildIndex(storePath string) error {
	storeFile, err := os.Open(storePath)
	if err != nil {
		return fmt.Errorf("failed to open store file: %w", err)
	}
	defer storeFile.Close()

	baseOffset, err := extractBaseOffset(storePath)
	if err != nil {
		return fmt.Errorf("invalid store filename: %w", err)
	}

	indexPath := fmt.Sprintf("%d.index", baseOffset)
	indexFile, err := os.Create(indexPath)
	if err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}
	defer indexFile.Close()

	var pos int64 = 0
	var relativeOffset uint32 = 0

	for {
		_, err := storeFile.Seek(pos, io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek store: %w", err)
		}

		lengthBuf := make([]byte, 8)
		_, err = io.ReadFull(storeFile, lengthBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record length: %w", err)
		}

		dataLen := binary.BigEndian.Uint64(lengthBuf)

		pos += 8
		_, err = storeFile.Seek(int64(dataLen), io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to seek over record data: %w", err)
		}

		entry := make([]byte, 12)
		binary.BigEndian.PutUint32(entry[:4], relativeOffset)
		binary.BigEndian.PutUint64(entry[4:], uint64(pos-8))

		if _, err := indexFile.Write(entry); err != nil {
			return fmt.Errorf("failed to write index: %w", err)
		}

		pos += int64(dataLen)
		relativeOffset++
	}

	return nil
}

func extractBaseOffset(filename string) (uint64, error) {
	base := filepath.Base(filename)
	trimmed := strings.TrimSuffix(base, ".store")
	return strconv.ParseUint(trimmed, 10, 64)
}
