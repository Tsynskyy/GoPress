package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type DecompressResult struct {
	Index int
	Data  []byte
}

type DecompressionOptions struct {
	InputPath  string
	OutputPath string
	NumWorkers int
	Offset     int64
	Size       int64
}

type BlockRange struct {
	StartBlock         int
	EndBlock           int
	StartOffsetInBlock int64
	EndOffsetInBlock   int64
}

func decompressFile(opts DecompressionOptions) error {
	// Opening the input file for reading
	inputFile, err := os.Open(opts.InputPath)
	if err != nil {
		return fmt.Errorf("Не удалось открыть входной файл: %v", err)
	}
	defer func() {
		if err := inputFile.Close(); err != nil {
			log.Println("Ошибка при закрытии входного файла:", err)
		}
	}()

	// Getting the size of the input file before unpacking
	inputFileSize, err := getFileSize(opts.InputPath)
	if err != nil {
		return fmt.Errorf("Не удалось получить размер входного файла: %v", err)
	}

	// Opening the output file for writing
	outputFile, err := os.Create(opts.OutputPath)
	if err != nil {
		return fmt.Errorf("Не удалось создать выходной файл: %v", err)
	}
	defer func() {
		if err := outputFile.Close(); err != nil {
			log.Println("Ошибка при закрытии выходного файла:", err)
		}
	}()

	// Reading the header
	header, err := readHeader(inputFile)
	if err != nil {
		return fmt.Errorf("Не удалось прочитать заголовок: %v", err)
	}

	// Calculating the total size of the original data
	var totalOriginalSize int64
	for _, block := range header.Blocks {
		totalOriginalSize += block.OriginalSize
	}

	// Validating offset and size
	if opts.Offset > totalOriginalSize {
		return fmt.Errorf("Параметр -offset выходит за пределы данных")
	}

	size := opts.Size

	if size > 0 && opts.Offset+size > totalOriginalSize {
		fmt.Println("Предупреждение: параметр -size выходит за пределы данных, распаковка будет до конца файла")
		size = -1
	}

	blockRange := calculateBlocks(header, opts.Offset, size)

	// Validating calculated blocks
	if blockRange.StartBlock < 0 || blockRange.StartBlock >= len(header.Blocks) {
		return fmt.Errorf("Вычисленный startBlock некорректен")
	}
	if blockRange.EndBlock < 0 || blockRange.EndBlock >= len(header.Blocks) {
		return fmt.Errorf("Вычисленный endBlock некорректен")
	}

	// Channels for tasks, results, and errors
	taskChan := make(chan int, opts.NumWorkers)
	resultChan := make(chan DecompressResult, opts.NumWorkers)
	errChan := make(chan error, opts.NumWorkers)
	var wg sync.WaitGroup

	// Starting worker goroutines to decompress blocks in parallel
	for i := 0; i < opts.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range taskChan {
				block := header.Blocks[index]

				// Reading a compressed block
				compressedData := make([]byte, block.CompressedSize)
				_, err := inputFile.ReadAt(compressedData, block.Offset)
				if err != nil {
					errChan <- fmt.Errorf("Ошибка при чтении блока %d: %v", index, err)
					return
				}

				// Unpacking the block
				reader, err := gzip.NewReader(bytes.NewReader(compressedData))
				if err != nil {
					errChan <- fmt.Errorf("Ошибка при распаковке блока %d: %v", index, err)
					return
				}
				decompressedData, err := io.ReadAll(reader)
				errClose := reader.Close()
				if err != nil || errClose != nil {
					if err == nil {
						err = errClose
					}
					errChan <- fmt.Errorf("Ошибка при чтении распакованных данных блока %d: %v", index, err)
					return
				}

				resultChan <- DecompressResult{
					Index: index,
					Data:  decompressedData,
				}
			}
		}()
	}

	// Sending unpacking tasks
	go func() {
		for i := blockRange.StartBlock; i <= blockRange.EndBlock; i++ {
			taskChan <- i
		}
		close(taskChan)
	}()

	// Collecting the unpacking results
	go func() {
		wg.Wait()
		close(resultChan)
		close(errChan)
	}()

	// Saving the unpacked data in the correct order
	resultsMap := make(map[int][]byte)
	expectedIndex := blockRange.StartBlock
	totalWritten := int64(0)

	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				resultChan = nil
			} else {
				resultsMap[result.Index] = result.Data

				// Writing blocks in the order of their sequence
				for {
					data, ok := resultsMap[expectedIndex]
					if !ok {
						break
					}

					// Cropping data for the start and end blocks
					if expectedIndex == blockRange.StartBlock && expectedIndex == blockRange.EndBlock {
						data = data[blockRange.StartOffsetInBlock:blockRange.EndOffsetInBlock]
					} else if expectedIndex == blockRange.StartBlock {
						data = data[blockRange.StartOffsetInBlock:]
					} else if expectedIndex == blockRange.EndBlock {
						data = data[:blockRange.EndOffsetInBlock]
					}

					n, err := outputFile.Write(data)
					if err != nil {
						return fmt.Errorf("Ошибка при записи данных: %v", err)
					}
					totalWritten += int64(n)
					delete(resultsMap, expectedIndex)
					expectedIndex++
				}
			}

		case err, ok := <-errChan:
			if !ok {
				errChan = nil
			} else {
				return err
			}
		}

		if resultChan == nil && errChan == nil {
			break
		}
	}

	// Getting the size of the unpacked file after unpacking is complete
	outputFileSize, err := getFileSize(opts.OutputPath)
	if err != nil {
		return fmt.Errorf("Не удалось получить размер выходного файла: %v", err)
	}

	fmt.Println("Распаковка завершена успешно.")
	fmt.Printf("Размер файла до распаковки: %d байт\n", inputFileSize)
	fmt.Printf("Размер файла после распаковки: %d байт\n", outputFileSize)

	return nil
}

func readHeader(file *os.File) (Header, error) {
	var header Header

	// Reading the signature
	var magic [4]byte
	_, err := file.ReadAt(magic[:], 0)
	if err != nil {
		return header, err
	}
	if magic != [4]byte{'C', 'M', 'P', '1'} {
		return header, fmt.Errorf("неверный формат файла")
	}

	// Reading the header size
	var headerSize int64
	_, err = file.Seek(4, io.SeekStart)
	if err != nil {
		return header, err
	}
	err = binary.Read(file, binary.LittleEndian, &headerSize)
	if err != nil {
		return header, err
	}

	// Validating the header size
	fileInfo, err := file.Stat()
	if err != nil {
		return header, err
	}
	if headerSize <= 0 || headerSize > fileInfo.Size() {
		return header, fmt.Errorf("некорректный размер заголовка")
	}

	// Calculating the offset of the header
	headerOffset := fileInfo.Size() - headerSize
	if headerOffset < 12 { // 4 bytes signature + 8 bytes header size
		return header, fmt.Errorf("некорректный заголовок")
	}

	// Reading the header
	_, err = file.Seek(headerOffset, io.SeekStart)
	if err != nil {
		return header, err
	}

	// Reading BlockSize and BlockCount
	err = binary.Read(file, binary.LittleEndian, &header.BlockSize)
	if err != nil {
		return header, err
	}
	err = binary.Read(file, binary.LittleEndian, &header.BlockCount)
	if err != nil {
		return header, err
	}

	// Reading blocks information
	header.Blocks = make([]BlockInfo, header.BlockCount)
	for i := int64(0); i < header.BlockCount; i++ {
		var block BlockInfo
		err = binary.Read(file, binary.LittleEndian, &block.Offset)
		if err != nil {
			return header, err
		}
		err = binary.Read(file, binary.LittleEndian, &block.CompressedSize)
		if err != nil {
			return header, err
		}
		err = binary.Read(file, binary.LittleEndian, &block.OriginalSize)
		if err != nil {
			return header, err
		}
		header.Blocks[i] = block
	}
	return header, nil
}

func calculateBlocks(header Header, offset, size int64) BlockRange {
	var br BlockRange
	blockSize := header.BlockSize
	br.StartBlock = int(offset / blockSize)
	br.StartOffsetInBlock = offset % blockSize

	var totalOriginalSize int64
	for _, block := range header.Blocks {
		totalOriginalSize += block.OriginalSize
	}

	var endOffset int64
	if size < 0 || offset+size > totalOriginalSize {
		endOffset = totalOriginalSize
	} else {
		endOffset = offset + size
	}

	br.EndBlock = int((endOffset - 1) / blockSize)
	br.EndOffsetInBlock = (endOffset-1)%blockSize + 1

	return br
}
