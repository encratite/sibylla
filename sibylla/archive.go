package sibylla

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	Momentum8 *float64
	Momentum24 *float64
	Momentum24Lag *float64
	Momentum48 *float64
	Returns24 *int
	Returns48 *int
	Returns72 *int
}

func writeArchive(symbol string, archive *Archive) {
	path := filepath.Join(configuration.GobPath, fmt.Sprintf("%s.%s", symbol, archiveExtension))
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
		log.Fatal("Failed to encode archive:", err)
	}
	fmt.Printf("Wrote archive to %s\n", path)
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