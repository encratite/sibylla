package sibylla

import (
	"fmt"
	"time"
)

type positionSide int

const (
	SideLong positionSide = iota
	SideShort
)

type assetPath struct {
	asset Asset
	path string
}

type assetRecords struct {
	asset Asset
	records []FeatureRecord
	recordsMap map[time.Time]*FeatureRecord
}

type correlationThreshold struct {
	asset assetRecords
	feature featureAccessor
	min float64
	max float64
}

type correlationTask [2]correlationThreshold

type correlationResult struct {
}

func Correlate(fromString, toString string) {
	loadConfiguration()
	from := getDateFromArg(fromString)
	to := getDateFromArg(toString)
	assetPaths := []assetPath{}
	for _, asset := range *assets {
		fRecords := 1
		if asset.FRecords != nil {
			fRecords = *asset.FRecords
		}
		for fNumber := 1; fNumber <= fRecords; fNumber++ {
			path := getArchivePath(asset.Symbol, fNumber)
			assetPath := assetPath{
				asset: asset,
				path: path,
			}
			assetPaths = append(assetPaths, assetPath)
		}
	}
	start := time.Now()
	assetRecords := parallelMap(assetPaths, func (ap assetPath) assetRecords {
		archive := readArchive(ap.path)
		records := []FeatureRecord{}
		recordsMap := map[time.Time]*FeatureRecord{}
		for _, record := range archive.IntradayRecords {
			if from != nil && record.Timestamp.Before(*from) {
				continue
			}
			if to != nil && !record.Timestamp.Before(*to) {
				break
			}
			records = append(records, record)
			recordsMap[record.Timestamp] = &record
		}
		return assetRecords{
			asset: ap.asset,
			records: records,
			recordsMap: recordsMap,
		}
	})
	delta := time.Since(start)
	fmt.Printf("Loaded archives in %.2f s\n", delta.Seconds())
	tasks := getCorrelationTasks(assetRecords)
	results := parallelMap(tasks, executeCorrelationTask)
	fmt.Printf("Results: %d\n", len(results))
}

func getCorrelationTasks(assetRecords []assetRecords) []correlationTask {
	accessors := getFeatureAccessors()
	minMax := [][]float64{
		{0.0, 0.3},
		{0.35, 0.65},
		{0.7, 1.0},
	}
	tasks := []correlationTask{}
	for i, asset1 := range assetRecords {
		if asset1.asset.FeaturesOnly {
			continue
		}
		for j, asset2 := range assetRecords {
			for k, feature1 := range accessors {
				for l, feature2 := range accessors {
					if i == j && k == l {
						continue
					}
					for _, minMax1 := range minMax {
						for _, minMax2 := range minMax {
							threshold1 := newCorrelationThreshold(asset1, feature1, minMax1)
							threshold2 := newCorrelationThreshold(asset2, feature2, minMax2)
							task := correlationTask{threshold1, threshold2}
							tasks = append(tasks, task)
						}
					}
				}
			}
		}
	}
	return tasks
}

func getDateFromArg(argument string) *time.Time {
	if argument != "" {
		date := getDate(argument)
		return &date
	} else {
		return nil
	}
}

func newCorrelationThreshold(asset assetRecords, feature featureAccessor, minMax []float64) correlationThreshold {
	return correlationThreshold{
		asset: asset,
		feature: feature,
		min: minMax[0],
		max: minMax[1],
	}
}

func executeCorrelationTask(task correlationTask) correlationResult {
	threshold1 := &task[0]
	threshold2 := &task[1]
	for _, record1 := range threshold1.asset.records {
		if !record1.hasReturns() || !threshold1.match(&record1) {
			continue
		}
		record2, exists := threshold2.asset.recordsMap[record1.Timestamp]
		if !exists || !threshold2.match(record2) {
			continue
		}
	}
	panic("Not implemented")
}

func (c *correlationThreshold) match(record *FeatureRecord) bool {
	pointer := c.feature.get(record)
	if pointer == nil {
		return false
	}
	value := *pointer
	match := value >= c.min && value <= c.max
	return match
}