package sibylla

import (
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
)

const archiveScript = "archive.js"
const dailyRecordsPlot = "daily.png"
const archiveMinNonNilValues = 1000

type ArchiveModel struct {
	Symbol string `json:"symbol"`
	Plot string `json:"plot"`
	Properties []PropertyStats `json:"properties"`
}

type PropertyStats struct {
	Name string `json:"name"`
	Plot string `json:"plot"`
	NilRatio float64 `json:"nilRatio"`
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	Mean float64 `json:"mean"`
	StdDev float64 `json:"stdDev"`
}

func ViewArchive(symbol string) {
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
	propertyStats := getPropertyStats(archive)
	model := ArchiveModel{
		Symbol: symbol,
		Plot: getFileURL(dailyRecordsPlotPath),
		Properties: propertyStats,
	}
	title := fmt.Sprintf("View Archive - %s", symbol)
	runBrowser(title, archiveScript, model)
}

func getPropertyStats(archive Archive) []PropertyStats {
	properties := getArchiveProperties()
	propertyStats := parallelMap(properties, func (feature archiveProperty) PropertyStats {
		return getPropertyStatsWorker(feature, archive)
	})
	return propertyStats
}

func getPropertyStatsWorker(feature archiveProperty, archive Archive) PropertyStats {
	values := []float64{}
	nilValues := 0
	min := math.Inf(1)
	max := math.Inf(-1)
	sum := 0.0
	for _, record := range archive.IntradayRecords {
		pointer := feature.get(&record)
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
		pointer := feature.get(&record)
		if pointer != nil {
			delta := *pointer - mean
			stdDevSum += delta * delta
		}
	}
	stdDev := math.Sqrt(stdDevSum / float64(len(values) - 1))
	fileName := fmt.Sprintf("%s.png", feature.name)
	plotPath := filepath.Join(configuration.TempPath, fileName)
	plotFeatureHistogram(stdDev, values, plotPath)
	return PropertyStats{
		Name: feature.name,
		Plot: getFileURL(plotPath),
		NilRatio: nilRatio,
		Min: min,
		Max: max,
		Mean: mean,
		StdDev: stdDev,
	}
}