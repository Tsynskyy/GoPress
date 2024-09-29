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

const DefaultBlockSize int64 = 1 << 20

type BlockInfo struct {
	Offset         int64
	CompressedSize int64
	OriginalSize   int64
}

type BlockRange struct {
	StartBlock         int
	EndBlock           int
	StartOffsetInBlock int64
	EndOffsetInBlock   int64
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
	flag.Int64Var(&blockSize, "bs", DefaultBlockSize, "Размер блока в байтах (по умолчанию 1MB)")
	flag.Parse()

	if compress == decompress {
		fmt.Println("Ошибка: необходимо выбрать либо сжатие (-c), либо распаковку (-d), но не оба сразу.")
		flag.Usage()
		os.Exit(1)
	}

	if inputPath == "" {
		fmt.Println("Ошибка: не указан путь к исходному файлу (-i).")
		flag.Usage()
		os.Exit(1)
	}

	if outputPath == "" {
		fmt.Println("Ошибка: не указан путь к выходному файлу (-o).")
		flag.Usage()
		os.Exit(1)
	}

	if numTasks < 1 {
		fmt.Println("Ошибка: параметр -n (число параллельных задач) должен быть >= 1")
		os.Exit(1)
	}

	if compress && blockSize <= 0 {
		fmt.Println("Ошибка: параметр -bs (размер блока) должен быть > 0")
		os.Exit(1)
	}

	if decompress && offset < 0 {
		fmt.Println("Ошибка: параметр -offset должен быть >= 0")
		os.Exit(1)
	}

	if decompress && size < -1 {
		fmt.Println("Ошибка: параметр -size должен быть >= -1 (или -1 для распаковки до конца файла)")
		os.Exit(1)
	}

	if inputPath == outputPath {
		fmt.Println("Ошибка: исходный и выходной файлы не должны совпадать")
		os.Exit(1)
	}

	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		fmt.Printf("Ошибка: файл %s не существует\n", inputPath)
		os.Exit(1)
	} else if err != nil {
		fmt.Printf("Ошибка при доступе к файлу %s: %v\n", inputPath, err)
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

	inputFileSize, err := getFileSize(inputPath)
	if err != nil {
		return fmt.Errorf("не удалось получить размер входного файла: %v", err)
	}

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

	currentOffset := int64(12) // 4 байта сигнатура + 8 байт размер заголовка

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

	go func() {
		index := 0
		for {
			buf := make([]byte, blockSize)

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

	outputFileSize, err := getFileSize(outputPath)
	if err != nil {
		return fmt.Errorf("не удалось получить размер выходного файла: %v", err)
	}

	fmt.Println("Сжатие завершено успешно.")
	fmt.Printf("Размер файла до сжатия: %d байт\n", inputFileSize)
	fmt.Printf("Размер файла после сжатия: %d байт\n", outputFileSize)

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
		return fmt.Errorf("не удалось открыть входной файл: %v", err)
	}
	defer func() {
		if err := inputFile.Close(); err != nil {
			log.Println("Ошибка при закрытии входного файла:", err)
		}
	}()

	inputFileSize, err := getFileSize(inputPath)
	if err != nil {
		return fmt.Errorf("не удалось получить размер входного файла: %v", err)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("не удалось создать выходной файл: %v", err)
	}
	defer func() {
		if err := outputFile.Close(); err != nil {
			log.Println("Ошибка при закрытии выходного файла:", err)
		}
	}()

	header, err := readHeader(inputFile)
	if err != nil {
		return fmt.Errorf("не удалось прочитать заголовок: %v", err)
	}

	var totalOriginalSize int64
	for _, block := range header.Blocks {
		totalOriginalSize += block.OriginalSize
	}

	if offset > totalOriginalSize {
		return fmt.Errorf("параметр -offset выходит за пределы данных")
	}

	if size > 0 && offset+size > totalOriginalSize {
		fmt.Println("Предупреждение: параметр -size выходит за пределы данных, будет распаковано до конца файла")
		size = -1
	}

	blockRange := calculateBlocks(header, offset, size)

	if blockRange.StartBlock < 0 || blockRange.StartBlock >= len(header.Blocks) {
		return fmt.Errorf("вычисленный startBlock некорректен")
	}
	if blockRange.EndBlock < 0 || blockRange.EndBlock >= len(header.Blocks) {
		return fmt.Errorf("вычисленный endBlock некорректен")
	}

	taskChan := make(chan int, numWorkers)
	resultChan := make(chan DecompressResult, numWorkers)
	errChan := make(chan error, numWorkers)
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
					errChan <- fmt.Errorf("Ошибка при чтении блока %d: %v", index, err)
					return
				}

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

	go func() {
		for i := blockRange.StartBlock; i <= blockRange.EndBlock; i++ {
			taskChan <- i
		}
		close(taskChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
		close(errChan)
	}()

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
				for {
					data, ok := resultsMap[expectedIndex]
					if !ok {
						break
					}

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

	outputFileSize, err := getFileSize(outputPath)
	if err != nil {
		return fmt.Errorf("не удалось получить размер выходного файла: %v", err)
	}

	fmt.Println("Распаковка завершена успешно.")
	fmt.Printf("Размер файла до распаковки: %d байт\n", inputFileSize)
	fmt.Printf("Размер файла после распаковки: %d байт\n", outputFileSize)

	return nil
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

func getFileSize(path string) (int64, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}
