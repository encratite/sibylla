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

type assetArchive struct {
	asset Asset
	archive Archive
}

type correlationThreshold struct {
	asset assetArchive
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
			ap := assetPath{
				asset: asset,
				path: path,
			}
			assetPaths = append(assetPaths, ap)
		}
	}
	start := time.Now()
	assetArchives := parallelMap(assetPaths, func (ap assetPath) assetArchive {
		archive := readArchive(ap.path)
		return assetArchive{
			asset: ap.asset,
			archive: archive,
		}
	})
	delta := time.Since(start)
	fmt.Printf("Loaded archives in %.2f s\n", delta.Seconds())
	accessors := getFeatureAccessors()
	minMax := [][]float64{
		{0.0, 0.3},
		{0.35, 0.65},
		{0.7, 1.0},
	}
	tasks := []correlationTask{}
	for i, asset1 := range assetArchives {
		if asset1.asset.FeaturesOnly {
			continue
		}
		for j, asset2 := range assetArchives {
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
	results := parallelMap(tasks, func (task correlationTask) correlationResult {
		return executeCorrelationTask(from, to, task)
	})
	fmt.Printf("Results: %d\n", len(results))
}

func getDateFromArg(argument string) *time.Time {
	if argument != "" {
		date := getDate(argument)
		return &date
	} else {
		return nil
	}
}

func newCorrelationThreshold(asset assetArchive, feature featureAccessor, minMax []float64) correlationThreshold {
	return correlationThreshold{
		asset: asset,
		feature: feature,
		min: minMax[0],
		max: minMax[1],
	}
}

func executeCorrelationTask(from *time.Time, to *time.Time, task correlationTask) correlationResult {
	panic("Not implemented")
}