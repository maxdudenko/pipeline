package main

import (
    "bufio"
    "fmt"
    "os"
    "strconv"
    "strings"
    "time"
)

const (
    bufferSize         = 5               // размер буфера
    bufferFlushPeriod  = 10 * time.Second // интервал опустошения буфера
)

func main() {
    inputChan := make(chan int)
    filteredChan1 := make(chan int)
    filteredChan2 := make(chan int)
    outputChan := make(chan int)
    done := make(chan struct{})

    // 1. Чтение с консоли
    go func() {
        scanner := bufio.NewScanner(os.Stdin)
        for {
            fmt.Print("Введите целое число (или exit): ")
            scanner.Scan()
            text := scanner.Text()
            if strings.ToLower(text) == "exit" {
                close(done)
                return
            }
            num, err := strconv.Atoi(text)
            if err != nil {
                fmt.Println("Ошибка: введите только целые числа")
                continue
            }
            inputChan <- num
        }
    }()

    // 2. Фильтрация отрицательных чисел
    go func() {
        for {
            select {
            case <-done:
                close(filteredChan1)
                return
            case num := <-inputChan:
                if num > 0 {
                    filteredChan1 <- num
                }
            }
        }
    }()

    // 3. Фильтрация по кратности 3 (исключая 0)
    go func() {
        for {
            select {
            case <-done:
                close(filteredChan2)
                return
            case num, ok := <-filteredChan1:
                if !ok {
                    return
                }
                if num%3 == 0 {
                    filteredChan2 <- num
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
                // При выходе — сбрасываем остатки буфера
                for _, val := range buffer {
                    outputChan <- val
                }
                close(outputChan)
                return
            case num := <-filteredChan2:
                if len(buffer) < bufferSize {
                    buffer = append(buffer, num)
                } else {
                    // удаляем самое старое значение и добавляем новое
                    buffer = append(buffer[1:], num)
                }
            case <-ticker.C:
                if len(buffer) > 0 {
                    for _, val := range buffer {
                        outputChan <- val
                    }
                    buffer = buffer[:0] // очищаем буфер
                }
            }
        }
    }()

    // 5. Потребитель
    for {
        select {
        case <-done:
            return
        case val, ok := <-outputChan:
            if !ok {
                return
            }
            fmt.Printf("Обработаны данные: %d\n", val)
        }
    }
}