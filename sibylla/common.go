package sibylla

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"
)

type taskTuple[T any] struct {
	index int
	element T
}

const dateLayout = "2006-01-02"
const timestampLayout = "2006-01-02 15:04"

func getDateErr(dateString string) (time.Time, error) {
	date, err := time.Parse(dateLayout, dateString)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse date string \"%s\": %v", dateString, err)
	}
	return date, nil
}

func getDate(dateString string) time.Time {
	date, err := getDateErr(dateString)
	if err != nil {
		log.Fatal(err)
	}
	return date
}

func getTimeErr(timeString string) (time.Time, error) {
	timestamp, err := time.Parse(timestampLayout, timeString)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse time string \"%s\": %v", timeString, err)
	}
	return timestamp, nil
}

func getTime(timeString string) time.Time {
	date, err := getTimeErr(timeString)
	if err != nil {
		log.Fatal(err)
	}
	return date
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

func getTimeOfDay(timestamp time.Time) time.Duration {
	duration := time.Duration(timestamp.Hour()) * time.Hour + time.Duration(timestamp.Minute()) * time.Minute
	return duration
}

func getTimeOfDayString(timeOfDay time.Duration) string {
	const minutesPerHour = 60
	hours := int(timeOfDay.Hours())
	minutes := int(timeOfDay.Minutes()) % minutesPerHour
	output := fmt.Sprintf("%02d:%02d", hours, minutes)
	return output
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

func clearDirectory(path string) {
	entries, err := os.ReadDir(path)
	if err != nil {
		log.Fatalf("Failed to read directory (%s): %v", path, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			filePath := filepath.Join(path, entry.Name())
			err := os.Remove(filePath)
			if err != nil {
				log.Fatalf("Failed to delete file (%s): %v", filePath, err)
			}
		}
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

func contains[T comparable](slice []T, element T) bool {
	for _, x := range slice {
		if x == element {
			return true
		}
	}
	return false
}

func find[T any](slice []T, match func (T) bool) (T, bool) {
	index := slices.IndexFunc(slice, func (element T) bool {
		return match(element)
	})
	if index >= 0 {
		return slice[index], true
	} else {
		var zeroValue T
		return zeroValue, false
	}
}

func compareFloat64(a, b float64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	} else {
		return 0
	}
}

func launchProfiler() {
	if configuration.ProfilerAddress != nil {
		go func() {
			err := http.ListenAndServe(*configuration.ProfilerAddress, nil)
			if err != nil {
				log.Fatal(err)
			}
		}()
	}
}

func parseFloat(floatString string) float64 {
	float, err := strconv.ParseFloat(floatString, 64)
	if err != nil {
		log.Fatalf("Failed to convert string \"%s\" to float: %v", floatString, err)
	}
	return float
}

func formatMoney(amount int64) string {
	amountString := fmt.Sprintf("%d", amount)
	output := "$"
	for i, character := range amountString {
		if i > 0 && (len(amountString) - i) % 3 == 0 {
			output += ","
		}
		output += string(character)
	}
	return output
}