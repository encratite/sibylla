package sibylla

import "time"

type openInterestRecord struct {
	symbol globexCode
	close serializableDecimal
	openInterest int
}

type intradayKey struct {
	symbol globexCode
	timestamp time.Time
}

type dailyRecord struct {
	date time.Time
	close serializableDecimal
}

type optionalFeature struct {
	value float64
	available bool
}

type optionalReturns struct {
	ticks int
	available bool
}

type featureRecord struct {
	timestamp time.Time
	momentum8 optionalFeature
	momentum24 optionalFeature
	momentum24_lag optionalFeature
	momentum48 optionalFeature
	returns24 optionalReturns
	returns48 optionalReturns
	returns72 optionalReturns
}

type serializedArchive struct {
	symbol string
	dailyRecords []dailyRecord
	intradayRecords []featureRecord
}

type openInterestMap map[time.Time][]openInterestRecord
type dailyGlobexMap map[time.Time]globexCode
type intradayRecordsMap map[intradayKey]serializableDecimal

func (f *featureRecord) includeRecord() bool {
	features := []optionalFeature{
		f.momentum8,
		f.momentum24,
		f.momentum24_lag,
		f.momentum48,
	}
	for _, x := range features {
		if x.available {
			return true
		}
	}
	returns := []optionalReturns{
		f.returns24,
		f.returns48,
		f.returns72,
	}
	for _, x := range returns {
		if x.available {
			return true
		}
	}
	return false
}