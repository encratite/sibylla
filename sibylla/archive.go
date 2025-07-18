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
	Close SerializableDecimal
}

type FeatureRecord struct {
	Timestamp time.Time
	Momentum1D *float64
	Momentum1DLag *float64
	Momentum2D *float64
	Momentum8H *float64
	Returns24H *int
	Returns48H *int
	Returns72H *int
}

type featureDefinition struct {
	name string
	selectFloat func (*FeatureRecord) *float64
}

type featureAccessor struct {
	get func (*FeatureRecord) *float64
	set func (*FeatureRecord, float64)
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

func getFeatureDefinitions() []featureDefinition {
	definitions := []featureDefinition{
		{
			name: "momentum1D",
			selectFloat: func (f *FeatureRecord) *float64 {
				return f.Momentum1D
			},
		},
		{
			name: "momentum1DLag",
			selectFloat: func (f *FeatureRecord) *float64 {
				return f.Momentum1DLag
			},
		},
		{
			name: "momentum2D",
			selectFloat: func (f *FeatureRecord) *float64 {
				return f.Momentum2D
			},
		},
		{
			name: "momentum8H",
			selectFloat: func (f *FeatureRecord) *float64 {
				return f.Momentum8H
			},
		},
		{
			name: "returns24H",
			selectFloat: func (f *FeatureRecord) *float64 {
				return selectIntToFloat(f.Returns24H)
			},
		},
		{
			name: "returns48H",
			selectFloat: func (f *FeatureRecord) *float64 {
				return selectIntToFloat(f.Returns48H)
			},
		},
		{
			name: "returns72H",
			selectFloat: func (f *FeatureRecord) *float64 {
				return selectIntToFloat(f.Returns72H)
			},
		},
	}
	return definitions
}

func selectIntToFloat(i *int) *float64 {
	if i != nil {
		f := float64(*i)
		return &f
	} else {
		return nil
	}
}

func getFeatureAccessors() []featureAccessor {
	accessors := []featureAccessor{
		{
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum1D
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum1D = &x
			},
		},
		{
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum1DLag
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum1DLag = &x
			},
		},
		{
			get: func (f *FeatureRecord) *float64 {
				return f.Momentum2D
			},
			set: func (f *FeatureRecord, x float64) {
				f.Momentum2D = &x
			},
		},
		{
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