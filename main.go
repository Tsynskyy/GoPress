package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
)

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

	if len(os.Args) > 1 && os.Args[1] == "help" {
		flag.Usage()
		os.Exit(0)
	}

	// Validating the input parameters
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

	// Performing compression or decompression
	if compress {
		opts := CompressionOptions{
			InputPath:  inputPath,
			OutputPath: outputPath,
			NumWorkers: numTasks,
			BlockSize:  blockSize,
		}
		err := compressFile(opts)
		if err != nil {
			log.Fatal("Ошибка при сжатии:", err)
		}
	} else if decompress {
		opts := DecompressionOptions{
			InputPath:  inputPath,
			OutputPath: outputPath,
			NumWorkers: numTasks,
			Offset:     offset,
			Size:       size,
		}
		err := decompressFile(opts)
		if err != nil {
			log.Fatal("Ошибка при распаковке:", err)
		}
	}
}
