package sibylla

import (
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

type openInterestRecords struct {
	date time.Time
	records []openInterestRecord
}

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
	parallelForEach(*assets, generateArchives)
	delta := time.Since(start)
	fmt.Printf("Generated archives in %.2f s\n", delta.Seconds())
}

func generateArchives(asset Asset) {
	openIntRecords, includedRecords, excludedRecords := readDailyRecords(asset)
	intradayRecords := readIntradayRecords(asset)
	intradayTimestampsMap := map[time.Time]struct{}{}
	for key := range intradayRecords {
		intradayTimestampsMap[key.timestamp] = struct{}{}
	}
	intradayTimestamps := []time.Time{}
	for key := range intradayTimestampsMap {
		intradayTimestamps = append(intradayTimestamps, key)
	}
	sort.Slice(intradayTimestamps, func (i, j int) bool {
		return intradayTimestamps[i].Before(intradayTimestamps[j])
	})
	totalRecords := includedRecords + excludedRecords
	exclusionRatio := float64(excludedRecords) / float64(totalRecords) * 100.0
	fmt.Printf("[%s] Excluded %.2f%% of records\n", asset.Symbol, exclusionRatio)
	/*
	limit := fRecordsLimit
	if asset.FRecordsLimit != nil {
		limit = *asset.FRecordsLimit
	}
	for fNumber := 1; fNumber <= limit; fNumber++ {
		generateFRecords(
			&fNumber,
			openIntRecords,
			intradayRecords,
			intradayTimestamps,
			asset,
		)
	}
	*/
	if asset.EnableFYRecords == nil || *asset.EnableFYRecords {
		generateFRecords(
			nil,
			openIntRecords,
			intradayRecords,
			intradayTimestamps,
			asset,
		)
	}
}

func generateFRecords(
	fNumber *int,
	openIntRecords []openInterestRecords,
	intradayRecords intradayRecordsMap,
	intradayTimestamps []time.Time,
	asset Asset,
) {
	dailyGlobexRecords := dailyGlobexMap{}
	dailyRecords := []DailyRecord{}
	for _, datedRecords := range openIntRecords {
		date := datedRecords.date
		fRecord := getFRecord(fNumber, date, datedRecords.records)
		if fRecord == nil {
			continue
		}
		dailyGlobexRecords[date] = fRecord.symbol
		dailyRecord := DailyRecord{
			Date: date,
			Close: fRecord.close,
		}
		dailyRecords = append(dailyRecords, dailyRecord)
	}
	archive := Archive{
		Symbol: asset.Symbol,
		DailyRecords: dailyRecords,
	}
	for _, timestamp := range intradayTimestamps {
		processIntradayTimestamp(
			timestamp,
			dailyGlobexRecords,
			intradayRecords,
			&asset,
			&archive,
		)
	}
	var suffix string
	if fNumber != nil {
		suffix = fmt.Sprintf("F%d", *fNumber)
	} else {
		suffix = "FY"
	}
	path, sizeBytes := writeArchive(asset.Symbol, suffix, &archive)
	sizeMibibytes := float64(sizeBytes) / 1024.0 / 1024.0
	fmt.Printf("[%s] Wrote archive to %s (%.1f MiB)\n", asset.Symbol, path, sizeMibibytes)
}

func processIntradayTimestamp(
	timestamp time.Time,
	dailyGlobexRecords dailyGlobexMap,
	intradayRecords intradayRecordsMap,
	asset *Asset,
	archive *Archive,
) {
	date := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, timestamp.Location())
	symbol, exists := dailyGlobexRecords[date]
	if !exists {
		// fmt.Printf("Missing date: %s\n", getDateString(date))
		return
	}
	// fmt.Printf("Found date: %s\n", getDateString(date))
	key := intradayKey{
		symbol: symbol,
		timestamp: timestamp,
	}
	close, exists := intradayRecords[key]
	if !exists {
		// fmt.Printf("intradayRecords lookup failed: %s %s\n", symbol, getTimeString(timestamp))
		return
	}
	fmt.Printf("intradayRecords lookup succeeded: %s %s\n", symbol, getTimeString(timestamp))
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

func getFRecord(fNumber *int, date time.Time, records []openInterestRecord) *openInterestRecord {
	root := records[0].symbol.Root
	if fNumber != nil {
		index := *fNumber - 1
		if index >= len(records) {
			fmt.Printf("[%s] Unable to determine F%d record at %s\n", root, *fNumber, getDateString(date))
			return nil
		}
		return &records[index]
	} else {
		firstSymbol := records[0].symbol
		for _, record := range records[1:] {
			symbol := record.symbol
			if symbol.Year > firstSymbol.Year && symbol.Month >= firstSymbol.Month {
				return &record
			}
		}
		fmt.Printf("[%s] Unable to determine FY record at %s\n", root, getDateString(date))
		return nil
	}
}

func readDailyRecords(asset Asset) ([]openInterestRecords, int, int) {
	path := getBarchartCsvPath(asset, "D1")
	columns := []string{"symbol", "time", "close", "open_interest"}
	openIntMap := openInterestMap{}
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
		if date.Before(configuration.CutoffDate.Time) {
			// Record is too old, skip it
			excludedRecords += 1
			return
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
		openIntMap[date] = append(openIntMap[date], openIntRecord)
		includedRecords += 1
	}
	readCsv(path, columns, callback)
	openIntRecords := []openInterestRecords{}
	for date, records := range openIntMap {
		sort.Slice(records, func (i, j int) bool {
			return records[i].openInterest > records[j].openInterest
		})
		datedRecords := openInterestRecords{
			date: date,
			records: records,
		}
		openIntRecords = append(openIntRecords, datedRecords)
	}
	sort.Slice(openIntRecords, func (i, j int) bool {
		return openIntRecords[i].date.Before(openIntRecords[j].date)
	})
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
		if timestamp.Before(configuration.CutoffDate.Time) {
			return
		}
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