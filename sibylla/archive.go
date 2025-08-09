package sibylla

import (
	"compress/gzip"
	"encoding/gob"
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
	Momentum1H *float64
	Momentum2H *float64
	Momentum4H *float64
	Momentum8H *float64
	Returns4H *ReturnsRecord
	Returns8H *ReturnsRecord
	Returns16H *ReturnsRecord
	Returns24H *ReturnsRecord
	Returns48H *ReturnsRecord
	Returns72H *ReturnsRecord
}

type ReturnsRecord struct {
	High int
	Low int
	Close1 int
	Close2 int
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
			name: "momentum1H",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum1H
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum1H = &x
			},
		},
		{
			name: "momentum2H",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum2H
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum2H = &x
			},
		},
		{
			name: "momentum4H",
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum4H
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum4H = &x
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
	}
	return accessors
}

func getReturnsAccessors() []returnsAccessor {
	accessors := []returnsAccessor{
		{
			name: "returns4H",
			holdingTime: 4,
			get: func (f *FeatureRecord) *ReturnsRecord {
				return f.Returns4H
			},
		},
		{
			name: "returns8H",
			holdingTime: 8,
			get: func (f *FeatureRecord) *ReturnsRecord {
				return f.Returns8H
			},
		},
		{
			name: "returns16H",
			holdingTime: 16,
			get: func (f *FeatureRecord) *ReturnsRecord {
				return f.Returns16H
			},
		},
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
				return f.Returns72H
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
					delta := pointer.Close2 - pointer.Close1
					value := float64(delta)
					return &value
				} else {
					return nil
				}
			},
		}
		properties = append(properties, ticksProperty)
	}
	return properties
}

func (f *FeatureRecord) hasReturns() bool {
	return f.Returns4H != nil ||
		f.Returns8H != nil ||
		f.Returns16H != nil ||
		f.Returns24H != nil ||
		f.Returns48H != nil ||
		f.Returns72H != nil
}