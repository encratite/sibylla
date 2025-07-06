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
	openInterestRecords := readOpenIntRecords(asset)
	dailyRecords := dailyRecordsMap{}
	for date, records := range openInterestRecords {
		maxRecord := getMaxOpenIntRecord(records)
		newRecord := dailyRecord{
			symbol: maxRecord.symbol,
			close: maxRecord.close,
		}
		dailyRecords[date] = newRecord
	}
	intradayRecords := readIntradayRecords(asset)
	intradayTimestamps := btree.NewG(32, timeLess)
	for key := range intradayRecords {
		intradayTimestamps.ReplaceOrInsert(key.timestamp)
	}
	intradayTimestamps.Ascend(func(timestamp time.Time) bool {
		date := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, timestamp.Location())
		record, exists := dailyRecords[date]
		if !exists {
			log.Fatalf("Missing daily record for date %s for symbol %s", getDateString(date), asset.getBarchartSymbol())
		}
		key := intradayKey{
			symbol: record.symbol,
			timestamp: timestamp,
		}
		close, exists := intradayRecords[key]
		if !exists {
			return true
		}
		momentum8 := getMomentum(8, 0, timestamp, close, record.symbol, intradayRecords)
		momentum24 := getMomentum(hoursPerDay, 0, timestamp, close, record.symbol, intradayRecords)
		momentum24_lag := getMomentum(hoursPerDay, hoursPerDay, timestamp, close, record.symbol, intradayRecords)
		momentum48 := getMomentum(2 * hoursPerDay, 0, timestamp, close, record.symbol, intradayRecords)
		returns24 := getReturns(hoursPerDay, timestamp, close, record.symbol, intradayRecords, &asset)
		returns48 := getReturns(2 * hoursPerDay, timestamp, close, record.symbol, intradayRecords, &asset)
		returns72 := getReturns(3 * hoursPerDay, timestamp, close, record.symbol, intradayRecords, &asset)
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

func readOpenIntRecords(asset Asset) openInterestMap {
	path := getBarchartCsvPath(asset, "D1")
	columns := []string{"symbol", "time", "close", "open_interest"}
	dailyRecords := openInterestMap{}
	callback := func(values []string) {
		symbol := globexFromString(values[0])
		date := getDate(values[1])
		close := getDecimal(values[2], path)
		openInterestString := values[3]
		openInterest, err := strconv.Atoi(openInterestString)
		if err != nil {
			log.Fatalf("Failed to parse open interest value \"%s\" in CSV file (%s): %v", openInterestString, path, err)
		}
		record := openInterestRecord{
			symbol: symbol,
			close: serializableDecimal(close),
			openInterest: openInterest,
		}
		dailyRecords[date] = append(dailyRecords[date], record)
	}
	readCsv(path, columns, callback)
	return dailyRecords
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
