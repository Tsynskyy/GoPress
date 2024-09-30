package main

import (
	"os"
)

const DefaultBlockSize int64 = 1 << 20 // 1 MB

type BlockInfo struct {
	Offset         int64 // Block offset in a compressed file
	CompressedSize int64 // Compressed block size
	OriginalSize   int64 // Original block size
}

type Header struct {
	Magic      [4]byte // File signature {'C', 'M', 'P', '1'}
	BlockSize  int64
	BlockCount int64
	Blocks     []BlockInfo
}

func getFileSize(path string) (int64, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}
