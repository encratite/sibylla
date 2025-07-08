package sibylla

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/btree"
	"github.com/shopspring/decimal"
)

type openInterestRecord struct {
	symbol GlobexCode
	close SerializableDecimal
	openInterest int
}

type intradayKey struct {
	symbol GlobexCode
	timestamp time.Time
}

type openInterestMap map[time.Time][]openInterestRecord
type dailyGlobexMap map[time.Time]GlobexCode
type intradayRecordsMap map[intradayKey]SerializableDecimal

func Generate() {
	loadConfiguration()
	start := time.Now()
	parallelForEach(*assets, generateArchive)
	delta := time.Since(start)
	fmt.Printf("Generated archives in %.2f s\n", delta.Seconds())
}

func generateArchive(asset Asset) {
	openIntRecords, includedRecords, excludedRecords := readDailyRecords(asset)
	dailyGlobexRecords := dailyGlobexMap{}
	dailyRecords := []DailyRecord{}
	for date, records := range openIntRecords {
		maxRecord := getMaxOpenIntRecord(records)
		dailyGlobexRecords[date] = maxRecord.symbol
		dailyRecord := DailyRecord{
			Date: date,
			Close: maxRecord.close,
		}
		dailyRecords = append(dailyRecords, dailyRecord)
	}
	intradayRecords := readIntradayRecords(asset)
	intradayTimestamps := btree.NewG(32, timeLess)
	for key := range intradayRecords {
		intradayTimestamps.ReplaceOrInsert(key.timestamp)
	}
	archive := Archive{
		Symbol: asset.Symbol,
		DailyRecords: dailyRecords,
	}
	intradayTimestamps.Ascend(func (timestamp time.Time) bool {
		return processIntradayTimestamp(
			timestamp,
			dailyGlobexRecords,
			intradayRecords,
			&asset,
			&archive,
		)
	})
	path := writeArchive(asset.Symbol, &archive)
	totalRecords := includedRecords + excludedRecords
	exclusionRatio := float64(excludedRecords) / float64(totalRecords) * 100.0
	fmt.Printf("Wrote archive to %s (%.2f%% of records excluded)\n", path, exclusionRatio)
}

func processIntradayTimestamp(
	timestamp time.Time,
	dailyGlobexRecords dailyGlobexMap,
	intradayRecords intradayRecordsMap,
	asset *Asset,
	archive *Archive,
) bool {
	date := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, timestamp.Location())
	symbol, exists := dailyGlobexRecords[date]
	if !exists {
		// There are some faulty intraday records for which no corresponding daily record exists
		// Skip it
		return true
	}
	key := intradayKey{
		symbol: symbol,
		timestamp: timestamp,
	}
	close, exists := intradayRecords[key]
	if !exists {
		return true
	}
	momentumHelper := func (offsetHours, lagHours int) *float64 {
		return getMomentum(offsetHours, lagHours, timestamp, close, symbol, intradayRecords)
	}
	returnsHelper := func (horizonHours int) *int {
		return getReturns(horizonHours, timestamp, close, symbol, intradayRecords, asset)
	}
	features := FeatureRecord{
		Timestamp: timestamp,
		Momentum8: momentumHelper(8, 0),
		Momentum24: momentumHelper(hoursPerDay, 0),
		Momentum24Lag: momentumHelper(hoursPerDay, hoursPerDay),
		Momentum48: momentumHelper(2 * hoursPerDay, 0),
		Returns24: returnsHelper(hoursPerDay),
		Returns48: returnsHelper(2 * hoursPerDay),
		Returns72: returnsHelper(3 * hoursPerDay),
	}
	if features.includeRecord() {
		archive.IntradayRecords = append(archive.IntradayRecords, features)
	}
	return true
}

func timeLess(a, b time.Time) bool {
	return a.Before(b)
}

func getMomentum(
	offsetHours int,
	lagHours int,
	timestamp time.Time,
	close SerializableDecimal,
	symbol GlobexCode,
	intradayRecords intradayRecordsMap,
) *float64 {
	offsetTimestamp := getAdjustedTimestamp(-offsetHours, timestamp)
	key := intradayKey{
		symbol: symbol,
		timestamp: offsetTimestamp,
	}
	offsetClose, exists := intradayRecords[key]
	if !exists {
		return nil
	}
	if lagHours > 0 {
		lagTimestamp := getAdjustedTimestamp(-lagHours, timestamp)
		key := intradayKey{
			symbol: symbol,
			timestamp: lagTimestamp,
		}
		lagClose, exists := intradayRecords[key]
		if !exists {
			return nil
		}
		close = lagClose
	}
	closeFloat, _ := close.Float64()
	offsetCloseFloat, _ := offsetClose.Float64()
	momentum, valid := getRateOfChange(closeFloat, offsetCloseFloat)
	if !valid {
		return nil
	}
	return &momentum
}

func getReturns(
	horizonHours int,
	timestamp time.Time,
	close SerializableDecimal,
	symbol GlobexCode,
	intradayRecords intradayRecordsMap,
	asset *Asset,
) *int {
	horizonTimestamp := getAdjustedTimestamp(horizonHours, timestamp)
	key := intradayKey{
		symbol: symbol,
		timestamp: horizonTimestamp,
	}
	horizonClose, exists := intradayRecords[key]
	if exists {
		delta := horizonClose.Sub(close.Decimal)
		ticks := int(delta.Div(asset.TickSize.Decimal).IntPart())
		return &ticks
	} else {
		return nil
	}
}

func getAdjustedTimestamp(offsetHours int, timestamp time.Time) time.Time {
	dayOffset := hoursPerDay
	if offsetHours < 0 {
		dayOffset = -dayOffset
	}
	adjustedTimestamp := timestamp.Add(time.Duration(offsetHours) * time.Hour)
	for {
		weekday := adjustedTimestamp.Weekday()
		if weekday == time.Saturday || weekday == time.Sunday {
			hour := adjustedTimestamp.Hour()
			nextDay := adjustedTimestamp.Add(time.Duration(dayOffset) * time.Hour)
			adjustedTimestamp = time.Date(
				nextDay.Year(),
				nextDay.Month(),
				nextDay.Day(),
				hour,
				0,
				0,
				0,
				nextDay.Location(),
			)
		} else {
			break
		}
	}
	return adjustedTimestamp
}

func getMaxOpenIntRecord(records []openInterestRecord) openInterestRecord {
	max := records[0]
	for _, record := range records[1:] {
		if record.openInterest > max.openInterest {
			max = record
		}
	}
	return max
}

func readDailyRecords(asset Asset) (openInterestMap, int, int) {
	path := getBarchartCsvPath(asset, "D1")
	columns := []string{"symbol", "time", "close", "open_interest"}
	openIntRecords := openInterestMap{}
	includedRecords := 0
	excludedRecords := 0
	callback := func(values []string) {
		symbol, err := parseGlobex(values[0])
		if err != nil {
			log.Fatal(err)
		}
		date, err := getDate(values[1])
		if err != nil {
			log.Fatal(err)
		}
		if !asset.includeRecord(date, symbol) {
			// The contract filter excludes the record, skip it
			excludedRecords += 1
			return
		}
		closeDecimal := getDecimal(values[2], path)
		close := SerializableDecimal(closeDecimal)
		openInterestString := values[3]
		openInterest, err := strconv.Atoi(openInterestString)
		if err != nil {
			log.Fatalf("Failed to parse open interest value \"%s\" in CSV file (%s): %v", openInterestString, path, err)
		}
		openIntRecord := openInterestRecord{
			symbol: symbol,
			close: close,
			openInterest: openInterest,
		}
		openIntRecords[date] = append(openIntRecords[date], openIntRecord)
		includedRecords += 1
	}
	readCsv(path, columns, callback)
	return openIntRecords, includedRecords, excludedRecords
}

func readIntradayRecords(asset Asset) intradayRecordsMap {
	path := getBarchartCsvPath(asset, "H1")
	columns := []string{"symbol", "time", "close"}
	recordsMap := intradayRecordsMap{}
	callback := func(values []string) {
		symbol, err := parseGlobex(values[0])
		if err != nil {
			log.Fatal(err)
		}
		timestamp := getTime(values[1])
		close := getDecimal(values[2], path)
		key := intradayKey{
			symbol: symbol,
			timestamp: timestamp,
		}
		recordsMap[key] = close
	}
	readCsv(path, columns, callback)
	return recordsMap
}

func getBarchartCsvPath(asset Asset, suffix string) string {
	symbol := asset.getBarchartSymbol()
	filename := fmt.Sprintf("%s.%s.csv", symbol, suffix)
	path := filepath.Join(configuration.BarchartPath, filename)
	return path
}

func getDecimal(s string, path string) SerializableDecimal {
	value, err := decimal.NewFromString(s)
	if err != nil {
		log.Fatalf("Failed to parse decimal string \"%s\" in CSV file (%s): %v", s, path, err)
	}
	return SerializableDecimal{
		Decimal: value,
	}
}

func (f *FeatureRecord) includeRecord() bool {
	features := []*float64{
		f.Momentum8,
		f.Momentum24,
		f.Momentum24Lag,
		f.Momentum48,
	}
	for _, x := range features {
		if x != nil {
			return true
		}
	}
	returns := []*int{
		f.Returns24,
		f.Returns48,
		f.Returns72,
	}
	for _, x := range returns {
		if x != nil {
			return true
		}
	}
	return false
}