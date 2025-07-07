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

func Generate() {
	loadConfiguration()
	parallelForEach(*assets, processAsset)
}

func processAsset(asset Asset) {
	openIntRecords := readDailyRecords(asset)
	dailyGlobexRecords := dailyGlobexMap{}
	dailyRecords := []dailyRecord{}
	for date, records := range openIntRecords {
		maxRecord := getMaxOpenIntRecord(records)
		dailyGlobexRecords[date] = maxRecord.symbol
		dailyRecord := dailyRecord{
			date: date,
			close: maxRecord.close,
		}
		dailyRecords = append(dailyRecords, dailyRecord)
	}
	intradayRecords := readIntradayRecords(asset)
	intradayTimestamps := btree.NewG(32, timeLess)
	for key := range intradayRecords {
		intradayTimestamps.ReplaceOrInsert(key.timestamp)
	}
	archive := serializedArchive{
		symbol: asset.Symbol,
		dailyRecords: dailyRecords,
	}
	intradayTimestamps.Ascend(func(timestamp time.Time) bool {
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
		momentumHelper := func (offsetHours, lagHours int) optionalFeature {
			return getMomentum(offsetHours, lagHours, timestamp, close, symbol, intradayRecords)
		}
		returnsHelper := func (horizonHours int) optionalReturns {
			return getReturns(horizonHours, timestamp, close, symbol, intradayRecords, &asset)
		}
		features := featureRecord{
			timestamp: timestamp,
			momentum8: momentumHelper(8, 0),
			momentum24: momentumHelper(hoursPerDay, 0),
			momentum24_lag: momentumHelper(hoursPerDay, hoursPerDay),
			momentum48: momentumHelper(2 * hoursPerDay, 0),
			returns24: returnsHelper(hoursPerDay),
			returns48: returnsHelper(2 * hoursPerDay),
			returns72: returnsHelper(3 * hoursPerDay),
		}
		if features.includeRecord() {
			archive.intradayRecords = append(archive.intradayRecords, features)
		}
		return true
	})
}

func timeLess(a, b time.Time) bool {
	return a.Before(b)
}

func getMomentum(
	offsetHours int,
	lagHours int,
	timestamp time.Time,
	close serializableDecimal,
	symbol globexCode,
	intradayRecords intradayRecordsMap,
) optionalFeature {
	notAvailable := optionalFeature{
		value: 0,
		available: false,
	}
	offsetTimestamp := getAdjustedTimestamp(-offsetHours, timestamp)
	key := intradayKey{
		symbol: symbol,
		timestamp: offsetTimestamp,
	}
	offsetClose, exists := intradayRecords[key]
	if !exists {
		return notAvailable
	}
	if lagHours > 0 {
		lagTimestamp := getAdjustedTimestamp(-lagHours, timestamp)
		key := intradayKey{
			symbol: symbol,
			timestamp: lagTimestamp,
		}
		lagClose, exists := intradayRecords[key]
		if !exists {
			return notAvailable
		}
		close = lagClose
	}
	closeFloat, _ := close.Float64()
	offsetCloseFloat, _ := offsetClose.Float64()
	momentum, valid := getRateOfChange(closeFloat, offsetCloseFloat)
	if !valid {
		return notAvailable
	}
	return optionalFeature{
		value: momentum,
		available: true,
	}
}

func getReturns(
	horizonHours int,
	timestamp time.Time,
	close serializableDecimal,
	symbol globexCode,
	intradayRecords intradayRecordsMap,
	asset *Asset,
) optionalReturns {
	horizonTimestamp := getAdjustedTimestamp(horizonHours, timestamp)
	key := intradayKey{
		symbol: symbol,
		timestamp: horizonTimestamp,
	}
	horizonClose, exists := intradayRecords[key]
	if exists {
		delta := horizonClose.Sub(close.Decimal)
		ticks := int(delta.Div(asset.TickSize.Decimal).IntPart())
		return optionalReturns{
			ticks: ticks,
			available: true,
		}
	} else {
		return optionalReturns{
			ticks: 0,
			available: false,
		}
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

func readDailyRecords(asset Asset) openInterestMap {
	path := getBarchartCsvPath(asset, "D1")
	columns := []string{"symbol", "time", "close", "open_interest"}
	openIntRecords := openInterestMap{}
	callback := func(values []string) {
		symbol := globexFromString(values[0])
		date := getDate(values[1])
		closeDecimal := getDecimal(values[2], path)
		close := serializableDecimal(closeDecimal)
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
	}
	readCsv(path, columns, callback)
	return openIntRecords
}

func readIntradayRecords(asset Asset) intradayRecordsMap {
	path := getBarchartCsvPath(asset, "H1")
	columns := []string{"symbol", "time", "close"}
	recordsMap := intradayRecordsMap{}
	callback := func(values []string) {
		symbol := globexFromString(values[0])
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

func getDecimal(s string, path string) serializableDecimal {
	value, err := decimal.NewFromString(s)
	if err != nil {
		log.Fatalf("Failed to parse decimal string \"%s\" in CSV file (%s): %v", s, path, err)
	}
	return serializableDecimal{
		Decimal: value,
	}
}
