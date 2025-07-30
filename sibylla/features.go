package sibylla

import (
	"cmp"
	"fmt"
	"log"
	"slices"
)

const enableFeatureAnalysis = true
const featureAnalysisLimit = 250

type featureStats struct {
	name  string
	counts [2]int
}

func analyzeFeatureFrequency(assetResults map[string][]dataMiningResult) {
	if !enableFeatureAnalysis {
		return
	}
	features := []featureStats{}
	accessors := getFeatureAccessors()
	for _, accessor := range accessors {
		feature := featureStats{
			name: accessor.name,
		}
		features = append(features, feature)
	}
	total := 0
	for symbol := range assetResults {
		slices.SortFunc(assetResults[symbol], func (a, b dataMiningResult) int {
			return compareFloat64(b.riskAdjustedMin, a.riskAdjustedMin)
		})
		results := assetResults[symbol]
		if len(results) > featureAnalysisLimit {
			results = results[:featureAnalysisLimit]
		}
		for i, result := range results {
			if i >= featureAnalysisLimit {
				break
			}
			for countIndex, threshold := range result.task {
				index := slices.IndexFunc(features, func (f featureStats) bool {
					return f.name == threshold.feature.name
				})
				if index == -1 {
					continue
				}
				features[index].counts[countIndex]++
				total++
			}
		}
	}
	fmt.Println("")
	for featureIndex := 0; featureIndex < 2; featureIndex++ {
		slices.SortFunc(features, func (a, b featureStats) int {
			return cmp.Compare(b.counts[featureIndex], a.counts[featureIndex])
		})
		fmt.Printf("Feature %d:\n", featureIndex + 1)
		for i, feature := range features {
			percentage := 100.0 * float64(feature.counts[featureIndex]) / float64(total)
			fmt.Printf("\t%d. %s: %.1f%%\n", i + 1, feature.name, percentage)
		}
		fmt.Println("")
	}
	fmt.Printf("Number of strategies evaluated per asset: %d\n\n", featureAnalysisLimit)
	log.Fatal("Analysis concluded")
}