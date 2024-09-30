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

type CompressTask struct {
	Index int
	Data  []byte
}

type CompressResult struct {
	Index        int
	Data         []byte
	OriginalSize int64
}

type CompressionOptions struct {
	InputPath  string
	OutputPath string
	NumWorkers int
	BlockSize  int64
}

func compressFile(opts CompressionOptions) error {
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

	// Getting the size of the input file before compressing
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

	_, err = outputFile.Write([]byte{'C', 'M', 'P', '1'})
	if err != nil {
		return err
	}

	err = binary.Write(outputFile, binary.LittleEndian, int64(0))
	if err != nil {
		return err
	}

	currentOffset := int64(12) // 4 bytes signature + 8 bytes header size

	// Channels for tasks and results
	taskChan := make(chan CompressTask, opts.NumWorkers)
	resultChan := make(chan CompressResult, opts.NumWorkers)
	var wg sync.WaitGroup

	// Starting worker goroutines to compress blocks in parallel
	for i := 0; i < opts.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskChan {
				var buf bytes.Buffer
				writer := gzip.NewWriter(&buf)
				_, err := writer.Write(task.Data)

				err = writer.Close()
				if err != nil {
					log.Println("Ошибка при сжатии:", err)
					continue
				}

				resultChan <- CompressResult{
					Index:        task.Index,
					Data:         buf.Bytes(),
					OriginalSize: int64(len(task.Data)),
				}
			}
		}()
	}

	// Reading the file and sending compression tasks
	go func() {
		index := 0
		for {
			buf := make([]byte, opts.BlockSize)

			n, err := inputFile.Read(buf)
			if err != nil && err != io.EOF {
				log.Println("Ошибка при чтении файла:", err)
				break
			}
			if n == 0 {
				break
			}
			taskChan <- CompressTask{
				Index: index,
				Data:  buf[:n],
			}
			index++
			if err == io.EOF {
				break
			}

		}
		close(taskChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Saving blocks information
	blockInfos := make([]BlockInfo, 0)
	resultsMap := make(map[int]CompressResult)
	expectedIndex := 0

	for result := range resultChan {
		resultsMap[result.Index] = result

		// Writing blocks in the order of their sequence
		for {
			res, ok := resultsMap[expectedIndex]
			if !ok {
				break
			}
			n, err := outputFile.Write(res.Data)
			if err != nil {
				return err
			}
			blockInfo := BlockInfo{
				Offset:         currentOffset,
				CompressedSize: int64(n),
				OriginalSize:   res.OriginalSize,
			}
			currentOffset += int64(n)
			blockInfos = append(blockInfos, blockInfo)
			expectedIndex++
			delete(resultsMap, res.Index)
		}
	}

	// Writing the header at the end of the file
	header := Header{
		Magic:      [4]byte{'C', 'M', 'P', '1'},
		BlockSize:  opts.BlockSize,
		BlockCount: int64(len(blockInfos)),
		Blocks:     blockInfos,
	}
	headerBytes, err := serializeHeader(header)
	if err != nil {
		return err
	}

	_, err = outputFile.Write(headerBytes)
	if err != nil {
		return err
	}
	headerSize := int64(len(headerBytes))

	// Writing the header size at the beginning of the file
	_, err = outputFile.Seek(4, io.SeekStart)
	if err != nil {
		return err
	}
	err = binary.Write(outputFile, binary.LittleEndian, headerSize)
	if err != nil {
		return err
	}

	// Getting the size of the compressed file after compression is complete
	outputFileSize, err := getFileSize(opts.OutputPath)
	if err != nil {
		return fmt.Errorf("Не удалось получить размер выходного файла: %v", err)
	}

	fmt.Println("Сжатие завершено успешно.")
	fmt.Printf("Размер файла до сжатия: %d байт\n", inputFileSize)
	fmt.Printf("Размер файла после сжатия: %d байт\n", outputFileSize)

	return nil
}

func serializeHeader(header Header) ([]byte, error) {
	var buf bytes.Buffer
	// Writing BlockSize and BlockCount
	err := binary.Write(&buf, binary.LittleEndian, header.BlockSize)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.LittleEndian, header.BlockCount)
	if err != nil {
		return nil, err
	}

	// Writing blocks information
	for _, block := range header.Blocks {
		err = binary.Write(&buf, binary.LittleEndian, block.Offset)
		if err != nil {
			return nil, err
		}
		err = binary.Write(&buf, binary.LittleEndian, block.CompressedSize)
		if err != nil {
			return nil, err
		}
		err = binary.Write(&buf, binary.LittleEndian, block.OriginalSize)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
