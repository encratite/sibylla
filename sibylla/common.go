package sibylla

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

type taskTuple[T any] struct {
	index int
	element T
}

const dateLayout = "2006-01-02"
const timestampLayout = "2006-01-02 15:04"

func getDate(dateString string) (time.Time, error) {
	date, err := time.Parse(dateLayout, dateString)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse date string \"%s\": %v", dateString, err)
	}
	return date, nil
}

func getTime(timeString string) time.Time {
	timestamp, err := time.Parse(timestampLayout, timeString)
	if err != nil {
		log.Fatalf("Failed to parse time string \"%s\": %v", timeString, err)
	}
	return timestamp
}

func getDateString(date time.Time) string {
	return date.Format(dateLayout)
}

func getTimeString(timestamp time.Time) string {
	return timestamp.Format(timestampLayout)
}

func getDateFromTime(timestamp time.Time) time.Time {
	date := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, timestamp.Location())
	return date
}

func parallelForEach[T any](elements []T, callback func(T)) {
	workers := runtime.NumCPU()
	elementChan := make(chan T, len(elements))
	for _, x := range elements {
		elementChan <- x
	}
	close(elementChan)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for element := range elementChan {
				callback(element)
			}
		}()
	}
	wg.Wait()
}

func parallelMap[A, B any](elements []A, callback func(A) B) []B {
	workers := runtime.NumCPU()
	elementChan := make(chan taskTuple[A], len(elements))
	for i, x := range elements {
		elementChan <- taskTuple[A]{
			index: i,
			element: x,
		}
	}
	close(elementChan)
	var wg sync.WaitGroup
	wg.Add(workers)
	output := make([]B, len(elements))
	for range workers {
		go func() {
			defer wg.Done()
			for task := range elementChan {
				output[task.index] = callback(task.element)
			}
		}()
	}
	wg.Wait()
	return output
}

func readFile(path string) []byte {
	content, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Failed to read file (%s): %v", path, err)
	}
	return content
}

func writeFile(path, data string) {
	bytes := []byte(data)
	err := os.WriteFile(path, bytes, 0644)
	if err != nil {
		log.Fatalf("Failed to write file (%s): %v", path, err)
	}
}

func readCsv(path string, columns []string, callback func([]string)) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Failed to read CSV file (%s): %v", path, err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		log.Fatal("Failed to read CSV headers", err)
	}
	headerMap := map[string]int{}
	for index, header := range headers {
		headerMap[header] = index
	}
	var indexMap []int
	for _, column := range columns {
		index, ok := headerMap[column]
		if !ok {
			log.Fatalf("Missing column \"%s\" in CSV file (%s)", column, path)
		}
		indexMap = append(indexMap, index)
	}
	callbackColumns := make([]string, len(columns))
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error occurred while reading CSV file (%s): %v", path, err)
		}
		for destination, source := range indexMap {
			callbackColumns[destination] = record[source]
		}
		callback(callbackColumns)
	}
}

func getRateOfChange(a, b float64) (float64, bool) {
	if a < 0 || b <= 0 {
		return 0, false
	}
	return a / b - 1, true
}