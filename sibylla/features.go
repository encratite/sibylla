package sibylla

import (
	"cmp"
	"fmt"
	"log"
	"slices"
)

const enableFeatureAnalysis = true
const featureAnalysisLimit = 250
const combinedFeatureLimit = 20

type featureStats struct {
	name string
	counts [2]int
}

type combinedFeatureStats struct {
	names [2]string
	count int
}

func analyzeFeatureFrequency(assetResults map[string][]dataMiningResult) {
	if !enableFeatureAnalysis {
		return
	}
	accessors := getFeatureAccessors()
	features := []featureStats{}
	for _, accessor := range accessors {
		feature := featureStats{
			name: accessor.name,
		}
		features = append(features, feature)
	}
	combinedFeatures := []combinedFeatureStats{}
	combinedFeaturesTotal := 0
	for _, accessor1 := range accessors {
		for _, accessor2 := range accessors {
			combinedFeature := combinedFeatureStats{
				names: [2]string{
					accessor1.name,
					accessor2.name,
				},
			}
			combinedFeatures = append(combinedFeatures, combinedFeature)
		}
	}
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
			for featureIndex, threshold := range result.task {
				index := slices.IndexFunc(features, func (f featureStats) bool {
					return f.name == threshold.feature.name
				})
				if index == -1 {
					continue
				}
				features[index].counts[featureIndex]++
			}
			index := slices.IndexFunc(combinedFeatures, func (c combinedFeatureStats) bool {
				return c.names[0] == result.task[0].feature.name &&
					c.names[1] == result.task[1].feature.name
			})
			if index == -1 {
				continue
			}
			combinedFeatures[index].count++
			combinedFeaturesTotal++
		}
	}
	fmt.Println("")
	for featureIndex := 0; featureIndex < 2; featureIndex++ {
		slices.SortFunc(features, func (a, b featureStats) int {
			return cmp.Compare(b.counts[featureIndex], a.counts[featureIndex])
		})
		fmt.Printf("Feature %d:\n", featureIndex + 1)
		total := 0
		for _, feature := range features {
			total += feature.counts[featureIndex]
		}
		for i, feature := range features {
			percentage := 100.0 * float64(feature.counts[featureIndex]) / float64(total)
			fmt.Printf("\t%d. %s: %.1f%%\n", i + 1, feature.name, percentage)
		}
		fmt.Println("")
	}
	slices.SortFunc(combinedFeatures, func (a, b combinedFeatureStats) int {
		return cmp.Compare(b.count, a.count)
	})
	fmt.Println("Combined features:")
	for i, cominbedFeature := range combinedFeatures {
		if i >= combinedFeatureLimit {
			break
		}
		percentage := 100.0 * float64(cominbedFeature.count) / float64(combinedFeaturesTotal)
		fmt.Printf("\t%d. %s, %s: %.1f%%\n", i + 1, cominbedFeature.names[0], cominbedFeature.names[1], percentage)
	}
	fmt.Printf("\nNumber of strategies evaluated per asset: %d\n\n", featureAnalysisLimit)
	log.Fatal("Analysis concluded")
}