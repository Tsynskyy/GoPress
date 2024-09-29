package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

type BlockInfo struct {
	Offset         int64
	CompressedSize int64
	OriginalSize   int64
}

type Header struct {
	Magic      [4]byte
	BlockSize  int64
	BlockCount int64
	Blocks     []BlockInfo
}

type CompressTask struct {
	Index int
	Data  []byte
}

type CompressResult struct {
	Index        int
	Data         []byte
	OriginalSize int64
}

type DecompressResult struct {
	Index int
	Data  []byte
}

func main() {
	var (
		compress   bool
		decompress bool
		inputPath  string
		outputPath string
		numTasks   int
		offset     int64
		size       int64
		blockSize  int64
	)

	flag.BoolVar(&compress, "c", false, "Сжатие файла")
	flag.BoolVar(&decompress, "d", false, "Распаковка файла")
	flag.StringVar(&inputPath, "i", "", "Исходный файл")
	flag.StringVar(&outputPath, "o", "", "Выходной файл")
	flag.IntVar(&numTasks, "n", runtime.GOMAXPROCS(0), "Число параллельных задач")
	flag.Int64Var(&offset, "offset", 0, "Начальный offset для распаковки")
	flag.Int64Var(&size, "size", -1, "Количество байт для распаковки")
	flag.Int64Var(&blockSize, "bs", 1<<20, "Размер блока в байтах (по умолчанию 1MB)")
	flag.Parse()

	if compress == decompress || inputPath == "" || outputPath == "" {
		fmt.Println("Использование:")
		fmt.Println("  Сжатие: -c -i input_file -o output_file [-n num_tasks] [-bs block_size]")
		fmt.Println("  Распаковка: -d -i input_file -o output_file [-n num_tasks] [-offset N] [-size M]")
		os.Exit(1)
	}

	if compress {
		err := compressFile(inputPath, outputPath, numTasks, blockSize)
		if err != nil {
			log.Fatal("Ошибка при сжатии:", err)
		}
	} else if decompress {
		err := decompressFile(inputPath, outputPath, numTasks, offset, size)
		if err != nil {
			log.Fatal("Ошибка при распаковке:", err)
		}
	}
}

func compressFile(inputPath, outputPath string, numWorkers int, blockSize int64) error {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	_, err = outputFile.Write([]byte{'C', 'M', 'P', '1'})
	if err != nil {
		return err
	}

	err = binary.Write(outputFile, binary.LittleEndian, int64(0))
	if err != nil {
		return err
	}

	currentOffset := int64(12)

	taskChan := make(chan CompressTask, numWorkers)
	resultChan := make(chan CompressResult, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskChan {
				var buf bytes.Buffer
				writer := gzip.NewWriter(&buf)
				_, err := writer.Write(task.Data)
				writer.Close()
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

	go func() {
		index := 0
		for {
			buf := make([]byte, blockSize)
			n, err := io.ReadFull(inputFile, buf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
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
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
		}
		close(taskChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	blockInfos := make([]BlockInfo, 0)
	resultsMap := make(map[int]CompressResult)
	expectedIndex := 0

	for result := range resultChan {
		resultsMap[result.Index] = result

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

	header := Header{
		Magic:      [4]byte{'C', 'M', 'P', '1'},
		BlockSize:  blockSize,
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

	_, err = outputFile.Seek(4, io.SeekStart)
	if err != nil {
		return err
	}
	err = binary.Write(outputFile, binary.LittleEndian, headerSize)
	if err != nil {
		return err
	}

	return nil
}

func serializeHeader(header Header) ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, header.BlockSize)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.LittleEndian, header.BlockCount)
	if err != nil {
		return nil, err
	}

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

func readHeader(file *os.File) (Header, error) {
	var header Header

	var magic [4]byte
	_, err := file.ReadAt(magic[:], 0)
	if err != nil {
		return header, err
	}
	if magic != [4]byte{'C', 'M', 'P', '1'} {
		return header, fmt.Errorf("неверный формат файла")
	}

	var headerSize int64

	_, err = file.Seek(4, io.SeekStart)
	if err != nil {
		return header, err
	}
	err = binary.Read(file, binary.LittleEndian, &headerSize)
	if err != nil {
		return header, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return header, err
	}
	if headerSize <= 0 || headerSize > fileInfo.Size() {
		return header, fmt.Errorf("некорректный размер заголовка")
	}

	headerOffset := fileInfo.Size() - headerSize
	if headerOffset < 12 {
		return header, fmt.Errorf("некорректный заголовок")
	}

	_, err = file.Seek(headerOffset, io.SeekStart)
	if err != nil {
		return header, err
	}

	err = binary.Read(file, binary.LittleEndian, &header.BlockSize)
	if err != nil {
		return header, err
	}
	err = binary.Read(file, binary.LittleEndian, &header.BlockCount)
	if err != nil {
		return header, err
	}

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

func decompressFile(inputPath, outputPath string, numWorkers int, offset, size int64) error {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	header, err := readHeader(inputFile)
	if err != nil {
		return err
	}

	startBlock, endBlock, startOffsetInBlock, endOffsetInBlock := calculateBlocks(header, offset, size)

	taskChan := make(chan int, numWorkers)
	resultChan := make(chan DecompressResult, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range taskChan {
				block := header.Blocks[index]

				compressedData := make([]byte, block.CompressedSize)
				_, err := inputFile.ReadAt(compressedData, block.Offset)
				if err != nil {
					log.Println("Ошибка при чтении блока:", err)
					continue
				}

				reader, err := gzip.NewReader(bytes.NewReader(compressedData))
				if err != nil {
					log.Println("Ошибка при распаковке:", err)
					continue
				}
				decompressedData, err := io.ReadAll(reader)
				reader.Close()
				if err != nil {
					log.Println("Ошибка при чтении распакованных данных:", err)
					continue
				}
				resultChan <- DecompressResult{
					Index: index,
					Data:  decompressedData,
				}
			}
		}()
	}

	go func() {
		for i := startBlock; i <= endBlock; i++ {
			taskChan <- i
		}
		close(taskChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	resultsMap := make(map[int][]byte)
	expectedIndex := startBlock
	totalWritten := int64(0)
	for result := range resultChan {
		resultsMap[result.Index] = result.Data

		for {
			data, ok := resultsMap[expectedIndex]
			if !ok {
				break
			}

			if expectedIndex == startBlock {
				data = data[startOffsetInBlock:]
			}
			if expectedIndex == endBlock && size >= 0 {
				end := int64(len(data))
				if endOffsetInBlock < end {
					data = data[:endOffsetInBlock]
				}
			}
			n, err := outputFile.Write(data)
			if err != nil {
				return err
			}
			totalWritten += int64(n)
			delete(resultsMap, expectedIndex)
			expectedIndex++
		}
	}

	return nil
}

func calculateBlocks(header Header, offset, size int64) (startBlock int, endBlock int, startOffsetInBlock int64, endOffsetInBlock int64) {
	blockSize := header.BlockSize
	startBlock = int(offset / blockSize)
	endByte := offset + size - 1
	if size < 0 {
		endByte = header.BlockCount*blockSize - 1
	}
	endBlock = int(endByte / blockSize)
	if endBlock >= int(header.BlockCount) {
		endBlock = int(header.BlockCount) - 1
	}
	startOffsetInBlock = offset % blockSize
	endOffsetInBlock = (endByte % blockSize) + 1
	return
}
