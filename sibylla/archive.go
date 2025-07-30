package sibylla

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"time"
)

const archiveExtension = "gobz"

type Archive struct {
	Symbol string
	DailyRecords []DailyRecord
	IntradayRecords []FeatureRecord
}

type DailyRecord struct {
	Date time.Time
	Close float64
}

type FeatureRecord struct {
	Timestamp time.Time
	Momentum1D *float64
	Momentum1DLag *float64
	Momentum2D *float64
	Momentum5D *float64
	Momentum10D *float64
	Momentum8H *float64
	Volatility10D *float64
	Volatility20D *float64
	Returns24H *ReturnsRecord
	Returns48H *ReturnsRecord
	Returns72H *ReturnsRecord
}

type ReturnsRecord struct {
	Ticks int
	Percent float64
}

type featureAccessor struct {
	name string
	anchored bool
	get func (*FeatureRecord) *float64
	set func (*FeatureRecord, float64)
}

type returnsAccessor struct {
	name string
	holdingTime int
	get func (*FeatureRecord) *ReturnsRecord
}

type archiveProperty struct {
	name string
	get func (*FeatureRecord) *float64
}

func readArchive(path string) Archive {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Failed to read archive %s: %v", path, err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		log.Fatalf("Failed to read gzip header from %s: %v", path, err)
	}
	defer reader.Close()
	decoder := gob.NewDecoder(reader)
	var archive Archive
	err = decoder.Decode(&archive)
	if err != nil {
		log.Fatalf("Failed to read decompressed Gob data from %s: %v", path, err)
	}
	return archive
}

func writeArchive(path string, archive *Archive) int64 {
	{
		file, err := os.Create(path)
		if err != nil {
			log.Fatalf("Failed to write archive to %s: %v", path, err)
		}
		defer file.Close()
		writer := gzip.NewWriter(file)
		defer writer.Close()
		encoder := gob.NewEncoder(writer)
		err = encoder.Encode(archive)
		if err != nil {
			log.Fatalf("Failed to encode archive %s: %v", path, err)
		}
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Fatalf("Failed to retrieve size of archive %s: %v", path, err)
	}
	size := fileInfo.Size()
	return size
}

func getFeatureAccessors() []featureAccessor {
	accessors := []featureAccessor{
		{
			name: "momentum1D",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum1D
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum1D = &x
			},
		},
		{
			name: "momentum1DLag",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum1DLag
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum1DLag = &x
			},
		},
		{
			name: "momentum2D",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum2D
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum2D = &x
			},
		},
		{
			name: "momentum5D",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum5D
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum5D = &x
			},
		},
		{
			name: "momentum10D",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum10D
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum10D = &x
			},
		},
		{
			name: "momentum8H",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum8H
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum8H = &x
			},
		},
		{
			name: "volatility10D",
			get: func (f *FeatureRecord) *float64 {
				return f.Volatility10D
			},
			set: func (f *FeatureRecord, x float64) {
				f.Volatility10D = &x
			},
		},
		{
			name: "volatility20D",
			get: func (f *FeatureRecord) *float64 {
				return f.Volatility20D
			},
			set: func (f *FeatureRecord, x float64) {
				f.Volatility20D = &x
			},
		},
	}
	return accessors
}

func getReturnsAccessors() []returnsAccessor {
	accessors := []returnsAccessor{
		{
			name: "returns24H",
			holdingTime: 24,
			get: func (f *FeatureRecord) *ReturnsRecord {
				return f.Returns24H
			},
		},
		{
			name: "returns48H",
			holdingTime: 48,
			get: func (f *FeatureRecord) *ReturnsRecord {
				return f.Returns48H
			},
		},
		{
			name: "returns72H",
			holdingTime: 72,
			get: func (f *FeatureRecord) *ReturnsRecord {
				return f.Returns48H
			},
		},
	}
	return accessors
}

func getArchiveProperties() []archiveProperty {
	properties := []archiveProperty{}
	featureAccessors := getFeatureAccessors()
	for _, feature := range featureAccessors {
		property := archiveProperty{
			name: feature.name,
			get: feature.get,
		}
		properties = append(properties, property)
	}
	returnsAccessors := getReturnsAccessors()
	for _, returns := range returnsAccessors {
		ticksProperty := archiveProperty{
			name: returns.name,
			get: func (f *FeatureRecord) *float64 {
				pointer := returns.get(f)
				if pointer != nil {
					value := float64(pointer.Ticks)
					return &value
				} else {
					return nil
				}
			},
		}
		percentageProperty := archiveProperty{
			name: fmt.Sprintf("%s (%%)", returns.name),
			get: func (f *FeatureRecord) *float64 {
				pointer := returns.get(f)
				if pointer != nil {
					return &pointer.Percent
				} else {
					return nil
				}
			},
		}
		properties = append(properties, ticksProperty, percentageProperty)
	}
	return properties
}

func (f *FeatureRecord) hasReturns() bool {
	return f.Returns24H != nil ||
		f.Returns48H != nil ||
		f.Returns72H != nil
}