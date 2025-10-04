package main

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"
    "time"
)

const (
    bufferSize        = 5                // размер буфера
    bufferFlushPeriod = 10 * time.Second // интервал опустошения буфера
)

func main() {
    // Настроим формат логов (по желанию)
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)

    inputChan := make(chan int)
    filteredChan1 := make(chan int)
    filteredChan2 := make(chan int)
    outputChan := make(chan int)
    done := make(chan struct{})

    log.Println("[main] starting pipeline")

    // 1. Чтение с консоли
    go func() {
        scanner := bufio.NewScanner(os.Stdin)
        for {
            fmt.Print("Введите целое число (или exit): ")
            if !scanner.Scan() {
                // EOF или ошибка сканера
                log.Println("[input] scanner stopped, closing done")
                close(done)
                return
            }
            text := scanner.Text()
            log.Printf("[input] received: %q", text)
            if strings.ToLower(text) == "exit" {
                log.Println("[input] exit received, closing done")
                close(done)
                return
            }
            num, err := strconv.Atoi(text)
            if err != nil {
                log.Printf("[input] invalid integer: %q", text)
                fmt.Println("Ошибка: введите только целые числа")
                continue
            }
            log.Printf("[input] sending: %d", num)
            inputChan <- num
        }
    }()

    // 2. Фильтрация отрицательных чисел
    go func() {
        for {
            select {
            case <-done:
                log.Println("[filter1] done signal, closing filteredChan1")
                close(filteredChan1)
                return
            case num := <-inputChan:
                log.Printf("[filter1] received: %d", num)
                if num > 0 {
                    log.Printf("[filter1] passed: %d", num)
                    filteredChan1 <- num
                } else {
                    log.Printf("[filter1] dropped (non-positive): %d", num)
                }
            }
        }
    }()

    // 3. Фильтрация по кратности 3 (исключая 0)
    go func() {
        for {
            select {
            case <-done:
                log.Println("[filter2] done signal, closing filteredChan2")
                close(filteredChan2)
                return
            case num, ok := <-filteredChan1:
                if !ok {
                    log.Println("[filter2] filteredChan1 closed, closing filteredChan2")
                    close(filteredChan2)
                    return
                }
                log.Printf("[filter2] received: %d", num)
                if num%3 == 0 {
                    log.Printf("[filter2] passed (multiple of 3): %d", num)
                    filteredChan2 <- num
                } else {
                    log.Printf("[filter2] dropped (not multiple of 3): %d", num)
                }
            }
        }
    }()

    // 4. Буферизация с таймером
    go func() {
        buffer := make([]int, 0, bufferSize)
        ticker := time.NewTicker(bufferFlushPeriod)
        defer ticker.Stop()

        for {
            select {
            case <-done:
                log.Println("[buffer] done signal, flushing buffer and closing outputChan")
                for _, val := range buffer {
                    log.Printf("[buffer] flushing on exit: %d", val)
                    outputChan <- val
                }
                close(outputChan)
                return
            case num, ok := <-filteredChan2:
                if !ok {
                    log.Println("[buffer] filteredChan2 closed, flushing buffer and closing outputChan")
                    for _, val := range buffer {
                        outputChan <- val
                    }
                    close(outputChan)
                    return
                }
                log.Printf("[buffer] received: %d", num)
                if len(buffer) < bufferSize {
                    buffer = append(buffer, num)
                    log.Printf("[buffer] appended: %d (len=%d)", num, len(buffer))
                } else {
                    evicted := buffer[0]
                    buffer = append(buffer[1:], num)
                    log.Printf("[buffer] evicted: %d, appended: %d", evicted, num)
                }
            case <-ticker.C:
                if len(buffer) > 0 {
                    log.Printf("[buffer] ticker tick, flushing %d items", len(buffer))
                    for _, val := range buffer {
                        outputChan <- val
                        log.Printf("[buffer] flushed: %d", val)
                    }
                    buffer = buffer[:0]
                } else {
                    log.Println("[buffer] ticker tick, buffer empty")
                }
            }
        }
    }()

    // 5. Потребитель — читаем из outputChan пока он не закроется
    for val := range outputChan {
        log.Printf("[consumer] got value: %d", val)
        fmt.Printf("Обработаны данные: %d\n", val)
    }

    log.Println("[main] outputChan closed, exiting")
}