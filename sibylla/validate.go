package sibylla

import (
	"fmt"
	"path/filepath"
	"strings"
)

type ValidationModel struct {
	Symbol string `json:"symbol"`
	Name string `json:"name"`
	Plot string `json:"plot"`
	Features []FeatureStats `json:"features"`
}

type FeatureStats struct {
	Name string `json:"name"`
	Plot string `json:"plot"`
	NilRatio float64 `json:"nilRatio"`
	Max float64 `json:"max"`
	Min float64 `json:"min"`
	Mean float64 `json:"Mean"`
	StdDev float64 `json:"stdDev"`
}

func ValidateArchive(symbol string) {
	loadConfiguration()
	var fileName string
	if strings.ContainsRune(symbol, '.') {
		fileName = fmt.Sprintf("%s.%s", symbol, archiveExtension)
	} else {
		fileName = fmt.Sprintf("%s.F1.%s", symbol, archiveExtension)
	}
	archivePath := filepath.Join(configuration.GobPath, fileName)
	archive := readArchive(archivePath)
	dailyRecordsPlotPath := filepath.Join(configuration.TempPath, fmt.Sprintf("%s.png", symbol))
	renderDailyRecords(symbol, archive.DailyRecords, dailyRecordsPlotPath)
}