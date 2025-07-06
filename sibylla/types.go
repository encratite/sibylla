package sibylla

import "time"

type openInterestRecord struct {
	symbol globexCode
	close serializableDecimal
	openInterest int
}

type dailyRecord struct {
	symbol globexCode
	close serializableDecimal
}

type intradayKey struct {
	symbol globexCode
	timestamp time.Time
}

type optionalFeature struct {
	value float64
	available bool
}

type optionalReturns struct {
	ticks int
	available bool
}

type openInterestMap map[time.Time][]openInterestRecord
type dailyRecordsMap map[time.Time]dailyRecord
type intradayRecordsMap map[intradayKey]serializableDecimal
