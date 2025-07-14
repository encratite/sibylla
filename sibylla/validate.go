package sibylla

import (
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
)

const validationScript = "validate.js"
const dailyRecordsPlot = "daily.png"
const archiveMinNonNilValues = 1000

type ValidationModel struct {
	Symbol string `json:"symbol"`
	Plot string `json:"plot"`
	Features []FeatureStats `json:"features"`
}

type FeatureStats struct {
	Name string `json:"name"`
	Plot string `json:"plot"`
	NilRatio float64 `json:"nilRatio"`
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	Mean float64 `json:"mean"`
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
	dailyRecordsPlotPath := filepath.Join(configuration.TempPath, dailyRecordsPlot)
	plotDailyRecords(archive.DailyRecords, dailyRecordsPlotPath)
	featureStats := getFeatureStats(archive)
	validationModel := ValidationModel{
		Symbol: symbol,
		Plot: getFileURL(dailyRecordsPlotPath),
		Features: featureStats,
	}
	title := fmt.Sprintf("Validate Archive - %s", symbol)
	runBrowser(title, validationScript, validationModel)
}

func getFeatureStats(archive Archive) []FeatureStats {
	featureDefinitions := getFeatureDefinitions()
	featureStats := parallelMap(featureDefinitions, func (feature featureDefinition) FeatureStats {
		return getFeatureStatsWorker(feature, archive)
	})
	return featureStats
}

func getFeatureStatsWorker(feature featureDefinition, archive Archive) FeatureStats {
	values := []float64{}
	nilValues := 0
	min := math.Inf(1)
	max := math.Inf(-1)
	sum := 0.0
	for _, record := range archive.IntradayRecords {
		pointer := feature.selectFloat(&record)
		if pointer != nil {
			value := *pointer
			if value < min {
				min = value
			}
			if value > max {
				max = value
			}
			sum += value
			values = append(values, value)
		} else {
			nilValues++
		}
	}
	if len(values) < archiveMinNonNilValues {
		log.Fatalf("Not enough non-nil values (%d) for feature %s in archive %s", len(values), feature.name, archive.Symbol)
	}
	nilRatio := float64(nilValues) / float64(len(values) + nilValues)
	mean := sum / float64(len(values))
	stdDevSum := 0.0
	for _, record := range archive.IntradayRecords {
		pointer := feature.selectFloat(&record)
		if pointer != nil {
			delta := *pointer - mean
			stdDevSum += delta * delta
		}
	}
	stdDev := math.Sqrt(stdDevSum / float64(len(values) - 1))
	fileName := fmt.Sprintf("%s.png", feature.name)
	plotPath := filepath.Join(configuration.TempPath, fileName)
	plotFeatureHistogram(stdDev, values, plotPath)
	return FeatureStats{
		Name: feature.name,
		Plot: getFileURL(plotPath),
		NilRatio: nilRatio,
		Min: min,
		Max: max,
		Mean: mean,
		StdDev: stdDev,
	}
}