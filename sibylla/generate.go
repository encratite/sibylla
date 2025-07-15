package sibylla

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

type openInterestRecords struct {
	date time.Time
	records []dailyRecord
}

type dailyRecord struct {
	symbol GlobexCode
	close SerializableDecimal
	openInterest int
}

type globexDateKey struct {
	symbol GlobexCode
	date time.Time
}

type globexTimeKey struct {
	symbol GlobexCode
	timestamp time.Time
}

type openInterestMap map[time.Time][]dailyRecord
type dailyRecordMap map[time.Time]dailyRecord
type dailyCloseMap map[globexDateKey]SerializableDecimal
type intradayCloseMap map[globexTimeKey]SerializableDecimal

func Generate(symbol *string) {
	loadConfiguration()
	start := time.Now()
	if symbol == nil {
		parallelForEach(*assets, func (asset Asset) {
			generateArchives(asset, false)
		})
	} else {
		generateSingleArchive(*symbol)
	}
	delta := time.Since(start)
	fmt.Printf("Generated archives in %.2f s\n", delta.Seconds())
}

func generateSingleArchive(symbol string) {
	for _, asset := range *assets {
		if asset.Symbol == symbol {
			generateArchives(asset, true)
			return
		}
	}
	log.Fatalf("Unable to find an asset matching symbol %s", symbol)
}

func generateArchives(asset Asset, forceOverwrite bool) {
	firstArchivePath := getArchivePath(asset.Symbol, 1)
	if !forceOverwrite && !configuration.OverwriteArchives {
		_, err := os.Stat(firstArchivePath)
		if !os.IsNotExist(err) {
			fmt.Printf("[%s] Archive already exists, skipping: %s\n", asset.Symbol, firstArchivePath)
			return
		}
	}
	openIntRecords, dailyCloses, includedRecords, excludedRecords := readDailyRecords(asset)
	intradayCloses := readIntradayRecords(asset)
	intradayTimestampsMap := map[time.Time]struct{}{}
	for key := range intradayCloses {
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
	fLimit := 1
	if asset.FRecords != nil {
		fLimit = *asset.FRecords
	}
	for fNumber := 1; fNumber <= fLimit; fNumber++ {
		generateFRecords(
			fNumber,
			openIntRecords,
			dailyCloses,
			intradayCloses,
			intradayTimestamps,
			asset,
		)
	}
}

func generateFRecords(
	fNumber int,
	openIntRecords []openInterestRecords,
	dailyCloses dailyCloseMap,
	intradayCloses intradayCloseMap,
	intradayTimestamps []time.Time,
	asset Asset,
) {
	path := getArchivePath(asset.Symbol, fNumber)
	dailyMap := dailyRecordMap{}
	dailyRecords := []DailyRecord{}
	for _, datedRecords := range openIntRecords {
		date := datedRecords.date
		fRecord := getFRecord(fNumber, date, datedRecords.records)
		if fRecord == nil {
			continue
		}
		dailyMap[date] = *fRecord
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
			dailyMap,
			dailyCloses,
			intradayCloses,
			&asset,
			&archive,
		)
	}
	sizeBytes := writeArchive(path, &archive)
	sizeMibibytes := float64(sizeBytes) / 1024.0 / 1024.0
	fmt.Printf("[%s] Wrote archive to %s (%.1f MiB)\n", asset.Symbol, path, sizeMibibytes)
}

func processIntradayTimestamp(
	timestamp time.Time,
	dailyRecords dailyRecordMap,
	dailyCloses dailyCloseMap,
	intradayCloses intradayCloseMap,
	asset *Asset,
	archive *Archive,
) {
	date := getDateFromTime(timestamp)
	record, exists := dailyRecords[date]
	if !exists {
		return
	}
	symbol := record.symbol
	key := getGlobexTimeKey(symbol, timestamp)
	close, exists := intradayCloses[key]
	if !exists {
		return
	}
	momentumHelper := func (offsetDays, lagDays, offsetHours int) *float64 {
		return getMomentum(offsetDays, lagDays, offsetHours, timestamp, close, symbol, dailyCloses, intradayCloses)
	}
	returnsHelper := func (offsetDays, offsetHours int) *int {
		return getReturns(offsetDays, offsetHours, timestamp, close, symbol, intradayCloses, asset)
	}
	closeTimestamp := timestamp.Add(time.Hour)
	features := FeatureRecord{
		Timestamp: closeTimestamp,
		Momentum1D: momentumHelper(1, 0, 0),
		Momentum2D: momentumHelper(2, 0, 0),
		Momentum2DLag: momentumHelper(2, 1, 0),
		Momentum8H: momentumHelper(0, 0, 8),
		Returns24H: returnsHelper(1, 0),
		Returns48H: returnsHelper(2, 0),
		Returns72H: returnsHelper(3, 0),
	}
	if features.includeRecord() {
		archive.IntradayRecords = append(archive.IntradayRecords, features)
	}
}

func getMomentum(
	offsetDays int,
	lagDays int,
	offsetHours int,
	timestamp time.Time,
	close SerializableDecimal,
	symbol GlobexCode,
	dailyCloses dailyCloseMap,
	intradayCloses intradayCloseMap,
) *float64 {
	offsetTimestamp := getAdjustedTimestamp(-offsetDays, -offsetHours, timestamp)
	var offsetClose SerializableDecimal
	if offsetHours == 0 {
		key := getGlobexDateKey(symbol, offsetTimestamp)
		dailyClose, exists := dailyCloses[key]
		if !exists {
			return nil
		}
		offsetClose = dailyClose
	} else {
		key := getGlobexTimeKey(symbol, offsetTimestamp)
		intradayClose, exists := intradayCloses[key]
		if !exists {
			return nil
		}
		offsetClose = intradayClose
	}
	if lagDays > 0 {
		lagTimestamp := getAdjustedTimestamp(-lagDays, 0, timestamp)
		key := getGlobexDateKey(symbol, lagTimestamp)
		lagClose, exists := dailyCloses[key]
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
	offsetDays int,
	offsetHours int,
	timestamp time.Time,
	close SerializableDecimal,
	symbol GlobexCode,
	intradayCloses intradayCloseMap,
	asset *Asset,
) *int {
	adjustedTimestamp := getAdjustedTimestamp(offsetDays, offsetHours, timestamp)
	key := getGlobexTimeKey(symbol, adjustedTimestamp)
	horizonClose, exists := intradayCloses[key]
	if exists {
		delta := horizonClose.Sub(close.Decimal)
		ticks := int(delta.Div(asset.TickSize.Decimal).IntPart())
		return &ticks
	} else {
		return nil
	}
}

func getAdjustedTimestamp(offsetDays int, offsetHours int, timestamp time.Time) time.Time {
	adjustedTimestamp := timestamp
	direction := 1
	if offsetDays < 0 {
		offsetDays = -offsetDays
		direction = -1
	}
	for range offsetDays {
		adjustedTimestamp = adjustedTimestamp.AddDate(0, 0, direction)
		for {
			weekday := adjustedTimestamp.Weekday()
			if weekday == time.Saturday || weekday == time.Sunday {
				adjustedTimestamp = adjustedTimestamp.AddDate(0, 0, direction)
			} else {
				break
			}
		}
	}
	if offsetHours != 0 {
		adjustedTimestamp = adjustedTimestamp.Add(time.Duration(offsetHours) * time.Hour)
	}
	return adjustedTimestamp
}

func getFRecord(fNumber int, date time.Time, records []dailyRecord) *dailyRecord {
	root := records[0].symbol.Root
	index := fNumber - 1
	if index >= len(records) {
		fmt.Printf("[%s] Unable to determine F%d record at %s\n", root, fNumber, getDateString(date))
		return nil
	}
	return &records[index]
}

func readDailyRecords(asset Asset) ([]openInterestRecords, dailyCloseMap, int, int) {
	path := getBarchartCsvPath(asset, "D1")
	columns := []string{"symbol", "time", "close", "open_interest"}
	openIntMap := openInterestMap{}
	dailyCloses := dailyCloseMap{}
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
		openIntRecord := dailyRecord{
			symbol: symbol,
			close: close,
			openInterest: openInterest,
		}
		openIntMap[date] = append(openIntMap[date], openIntRecord)
		key := getGlobexDateKey(symbol, date)
		dailyCloses[key] = close
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
	return openIntRecords, dailyCloses, includedRecords, excludedRecords
}

func readIntradayRecords(asset Asset) intradayCloseMap {
	path := getBarchartCsvPath(asset, "H1")
	columns := []string{"symbol", "time", "close"}
	recordsMap := intradayCloseMap{}
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
		key := getGlobexTimeKey(symbol, timestamp)
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
		f.Momentum1D,
		f.Momentum2D,
		f.Momentum2DLag,
		f.Momentum8H,
	}
	for _, x := range features {
		if x != nil {
			return true
		}
	}
	returns := []*int{
		f.Returns24H,
		f.Returns48H,
		f.Returns72H,
	}
	for _, x := range returns {
		if x != nil {
			return true
		}
	}
	return false
}

func getGlobexDateKey(symbol GlobexCode, timestamp time.Time) globexDateKey {
	date := getDateFromTime(timestamp)
	return globexDateKey{
		symbol: symbol,
		date: date,
	}
}

func getGlobexTimeKey(symbol GlobexCode, timestamp time.Time) globexTimeKey {
	return globexTimeKey{
		symbol: symbol,
		timestamp: timestamp,
	}
}

func getArchivePath(symbol string, fNumber int) string {
	suffix := fmt.Sprintf("F%d", fNumber)
	fileName := fmt.Sprintf("%s.%s.%s", symbol, suffix, archiveExtension)
	path := filepath.Join(configuration.GobPath, fileName)
	return path
}