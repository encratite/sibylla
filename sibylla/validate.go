package sibylla

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
)

const dailyRecordsPlot = "daily.png"
const validateTemplate = "validate.html"
const jsonPlaceholder = "MODEL_JSON"
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
	Max float64 `json:"max"`
	Min float64 `json:"min"`
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
	plotDailyRecords(symbol, archive.DailyRecords, dailyRecordsPlotPath)
	featureStats := getFeatureStats(archive)
	validationModel := ValidationModel{
		Symbol: symbol,
		Plot: getFileURL(dailyRecordsPlotPath),
		Features: featureStats,
	}
	jsonBytes, err := json.Marshal(validationModel)
	if err != nil {
		log.Fatal("Failed to serialize validation model to JSON:", err)
	}
	jsonString := string(jsonBytes)
	templatePath := filepath.Join(configuration.WebPath, validateTemplate)
	templateBytes := readFile(templatePath)
	templateString := string(templateBytes)
	templateString = strings.Replace(templateString, jsonPlaceholder, jsonString, 1)
	fmt.Println(templateString)
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
	max := math.Inf(-1)
	min := math.Inf(1)
	sum := 0.0
	for _, record := range archive.IntradayRecords {
		pointer := feature.selectFloat(&record)
		if pointer != nil {
			value := *pointer
			if value > max {
				max = value
			}
			if value < min {
				min = value
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
	plotFeatureHistogram(feature.name, stdDev, values, plotPath)
	return FeatureStats{
		Name: feature.name,
		Plot: getFileURL(plotPath),
		NilRatio: nilRatio,
		Max: max,
		Min: min,
		Mean: mean,
		StdDev: stdDev,
	}
}