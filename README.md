# GoPress

Данная утилита предназначена для сжатия и распаковки файлов с использованием алгоритма gzip. Она поддерживает многопоточную обработку, что позволяет значительно ускорить процессы сжатия и распаковки, особенно на больших файлах, а также позволяет распаковывать только определенные части файла, используя параметры смещения (`offset`) и размера (`size`).

## Особенности

- **Сжатие файлов** с возможностью указания размера блока и количества параллельных задач.
- **Распаковка файлов** с возможностью указания смещения и размера для частичной распаковки.
- **Многопоточность** для повышения производительности за счет параллельной обработки блоков.
- **Поддержка больших файлов** благодаря разбитию на блоки и эффективной обработке.

## Использование

### Синтаксис командной строки

```
Сжатие:
  -c -i input_file -o output_file [-n num_tasks] [-bs block_size]

Распаковка:
  -d -i input_file -o output_file [-n num_tasks] [-offset N] [-size M]
```

### Описание параметров

| Параметр     | Описание                                                                        | По умолчанию                                       |
| ------------ | ------------------------------------------------------------------------------- | -------------------------------------------------- |
| `-c`         | Режим сжатия файла.                                                             | `false`                                            |
| `-d`         | Режим распаковки файла.                                                         | `false`                                            |
| `-i`         | Путь к исходному файлу *(обязательно)*.                                         | `""`                                               |
| `-o`         | Путь к выходному файлу *(обязательно)*.                                         | `""`                                               |
| `-n`         | Число параллельных задач (потоков) *(необязательно)*.                           | `GOMAXPROCS` (Количество логических ядер системы)  |
| `-bs`        | Размер блока в байтах для сжатия *(необязательно)*.                             | `1MB` (1048576 байт)                               |
| `-offset`    | Смещение в байтах от начала файла, с которого начинается распаковка.            | `0`                                                |
| `-size`      | Количество байт для распаковки *(необязательно)*.                               | `-1` (до конца файла)                              |

### Примеры использования (Linux / macOS (Bash))

#### Сжатие файла

Для сжатия файла `source.txt` и сохранения результата в `compressed.dat` используя 4 параллельные задачи и размер блока 2 МБ:

```
./compressor -c -i source.txt -o compressed.dat -n 4 -bs 2097152
```

#### Распаковка всего файла

Для распаковки файла `compressed.dat` и сохранения результата в `restored.txt`:

```
./compressor -d -i compressed.dat -o restored.txt
```

#### Частичная распаковка

Для распаковки 1000 байт из `compressed.dat`, начиная со смещения 5000 байт, и сохранения результата в `restored_part.txt`:

```
./compressor -d -i compressed.dat -o restored_part.txt -offset 5000 -size 1000
```

#### Распаковка с использованием 8 параллельных задач

Для распаковки файла `compressed.dat` с использованием 8 параллельных задач и сохранения результата в `restored.txt`:

```
./compressor -d -i compressed.dat -o restored.txt -n 8
```

## Сборка и запуск
  
### Требования
  
- Go версии 1.16 или выше.
  
### Клонирование репозитория
  
```
git clone https://github.com/Tsynskyy/GoPress.git
cd GoPress
```
  
### Для Linux / macOS (Bash)
  
Сборка программы в исполняемый файл:
  
```
go build -o compressor
```
  
Запуск сжатия:
  
```
./compressor -c -i source.txt -o compressed.dat
```
  
Запуск распаковки:
  
```
./compressor -d -i compressed.dat -o restored.txt
```
  
### Для Windows (PowerShell)
  
Сборка программы в исполняемый файл:
  
```
go build -o compressor.exe
```
  
Запуск сжатия:
  
```
.\compressor.exe -c -i source.txt -o compressed.dat
```
  
Запуск распаковки:
  
```
.\compressor.exe -d -i compressed.dat -o restored.txt
```
  
### Для Windows (CMD)
  
Сборка программы в исполняемый файл:
  
```
go build -o compressor.exe
```
  
Запуск сжатия:

```
compressor.exe -c -i source.txt -o compressed.dat
```
  
Запуск распаковки:
  
```
compressor.exe -d -i compressed.dat -o restored.txt
```

### Запуск без предварительной сборки (Linux / macOS / Windows):

Запуск сжатия:

```
go run main.go compress.go decompress.go utils.go -c -i source.txt -o compressed.dat
```

Запуск распаковки:

```
go run main.go compress.go decompress.go utils.go -d -i compressed.dat -o restored.txt
```

---

# GoPress

This utility is designed for compressing and decompressing files using the gzip algorithm. It supports multithreaded processing, which allows significantly speeding up compression and decompression processes, especially for large files. It also allows decompressing only specific parts of the file using the offset and size parameters.

## Features

- **File compression** with the ability to specify block size and the number of parallel tasks.
- **File decompression** with the ability to specify offset and size for partial decompression.
- **Multithreading** to improve performance through parallel block processing.
- **Support for large files** by splitting them into blocks and processing them efficiently.

## Usage

### Command line syntax

```
Compression:
  -c -i input_file -o output_file [-n num_tasks] [-bs block_size]

Decompression:
  -d -i input_file -o output_file [-n num_tasks] [-offset N] [-size M]
```

### Parameter descriptions

| Parameter     | Description                                                                       | Default Value                                      |
| ------------- | --------------------------------------------------------------------------------- | -------------------------------------------------- |
| `-c`          | Compression mode.                                                                 | `false`                                            |
| `-d`          | Decompression mode.                                                               | `false`                                            |
| `-i`          | Path to the input file *(required)*.                                              | `""`                                               |
| `-o`          | Path to the output file *(required)*.                                             | `""`                                               |
| `-n`          | Number of parallel tasks (threads) *(optional)*.                                  | `GOMAXPROCS` (Number of logical CPU cores)         |
| `-bs`         | Block size in bytes for compression *(optional)*.                                 | `1MB` (1048576 bytes)                              |
| `-offset`     | Offset in bytes from the start of the file where decompression begins.            | `0`                                                |
| `-size`       | Number of bytes to decompress *(optional)*.                                       | `-1` (to the end of the file)                      |

### Usage examples (Linux / macOS (Bash))

#### Compressing a file

To compress the file `source.txt` and save the result as `compressed.dat` using 4 parallel tasks and a block size of 2 MB:

```
./compressor -c -i source.txt -o compressed.dat -n 4 -bs 2097152
```

#### Decompressing the entire file

To decompress the file `compressed.dat` and save the result as `restored.txt`:

```
./compressor -d -i compressed.dat -o restored.txt
```

#### Partial decompression

To decompress 1000 bytes from `compressed.dat` starting at the 5000-byte offset and save the result as `restored_part.txt`:

```
./compressor -d -i compressed.dat -o restored_part.txt -offset 5000 -size 1000
```

#### Decompression using 8 parallel tasks

To decompress the file `compressed.dat` using 8 parallel tasks and save the result as `restored.txt`:

```
./compressor -d -i compressed.dat -o restored.txt -n 8
```

## Building and running
  
### Requirements
  
- Go version 1.16 or higher.
  
### Cloning the repository
  
```
git clone https://github.com/Tsynskyy/GoPress.git
cd GoPress
```
  
### For Linux / macOS (Bash)
  
Build the program into an executable file:
  
```
go build -o compressor
```
  
Run compression:
  
```
./compressor -c -i source.txt -o compressed.dat
```
  
Run decompression:
  
```
./compressor -d -i compressed.dat -o restored.txt
```
  
### For Windows (PowerShell)
  
Build the program into an executable file:
  
```
go build -o compressor.exe
```
  
Run compression:
  
```
.\compressor.exe -c -i source.txt -o compressed.dat
```
  
Run decompression:
  
```
.\compressor.exe -d -i compressed.dat -o restored.txt
```
  
### For Windows (CMD)
  
Build the program into an executable file:
  
```
go build -o compressor.exe
```
  
Run compression:
  
```
compressor.exe -c -i source.txt -o compressed.dat
```
  
Run decompression:
  
```
compressor.exe -d -i compressed.dat -o restored.txt
```

### Run without prior building (Linux / macOS / Windows):

Run compression:

```
go run main.go compress.go decompress.go utils.go -c -i source.txt -o compressed.dat
```

Run decompression:

```
go run main.go compress.go decompress.go utils.go -d -i compressed.dat -o restored.txt
```
