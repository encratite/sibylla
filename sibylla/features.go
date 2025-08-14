package sibylla

import (
	"cmp"
	"fmt"
	"log"
	"slices"
	"strings"
)

const enableFeatureAnalysis = false
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

type featureAnalysis struct {
	features []featureStats
	combinedFeatures []combinedFeatureStats
	combinedFeaturesTotal int
}

func analyzeFeatureFrequency(assetResults map[string][]backtestData, miningConfig DataMiningConfiguration) *featureAnalysis {
	if miningConfig.SeasonalityMode {
		return nil
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
		slices.SortFunc(assetResults[symbol], func (a, b backtestData) int {
			return compareFloat64(b.minSharpe, a.minSharpe)
		})
		results := assetResults[symbol]
		if len(results) > featureAnalysisLimit {
			results = results[:featureAnalysisLimit]
		}
		for i, result := range results {
			if i >= featureAnalysisLimit {
				break
			}
			for featureIndex, parameter := range result.conditions {
				index := slices.IndexFunc(features, func (f featureStats) bool {
					return f.name == parameter.feature.name
				})
				if index == -1 {
					continue
				}
				features[index].counts[featureIndex]++
			}
			index := slices.IndexFunc(combinedFeatures, func (c combinedFeatureStats) bool {
				return c.names[0] == result.conditions[0].feature.name &&
					c.names[1] == result.conditions[1].feature.name
			})
			if index == -1 {
				continue
			}
			combinedFeatures[index].count++
			combinedFeaturesTotal++
		}
	}
	analysis := featureAnalysis{
		features: features,
		combinedFeatures: combinedFeatures,
		combinedFeaturesTotal: combinedFeaturesTotal,
	}
	if enableFeatureAnalysis {
		printFeatureFrequency(analysis, miningConfig)
	}
	return &analysis
}

func printFeatureFrequency(analysis featureAnalysis, miningConfig DataMiningConfiguration) {
	features := analysis.features
	combinedFeatures := analysis.combinedFeatures
	fmt.Println("")
	for featureIndex := 0; featureIndex < 2; featureIndex++ {
		sortedFeatures := make([]featureStats, len(features))
		copy(sortedFeatures, features)
		slices.SortFunc(sortedFeatures, func (a, b featureStats) int {
			return cmp.Compare(b.counts[featureIndex], a.counts[featureIndex])
		})
		fmt.Printf("Feature %d:\n", featureIndex + 1)
		total := 0
		for _, feature := range sortedFeatures {
			total += feature.counts[featureIndex]
		}
		for i, feature := range sortedFeatures {
			percentage := 100.0 * float64(feature.counts[featureIndex]) / float64(total)
			fmt.Printf("\t%d. %s: %.1f%%\n", i + 1, feature.name, percentage)
		}
		fmt.Println("")
	}
	sortedCombinedFeatures := make([]combinedFeatureStats, len(combinedFeatures))
	copy(sortedCombinedFeatures, combinedFeatures)
	slices.SortFunc(sortedCombinedFeatures, func (a, b combinedFeatureStats) int {
		return cmp.Compare(b.count, a.count)
	})
	fmt.Println("Combined features:")
	for i, cominbedFeature := range sortedCombinedFeatures {
		if i >= combinedFeatureLimit {
			break
		}
		percentage := 100.0 * float64(cominbedFeature.count) / float64(analysis.combinedFeaturesTotal)
		fmt.Printf("\t%d. %s, %s: %.1f%%\n", i + 1, cominbedFeature.names[0], cominbedFeature.names[1], percentage)
	}
	fmt.Printf("\nNumber of strategies evaluated per asset: %d\n", featureAnalysisLimit)
	symbolsEvaluated := strings.Join(miningConfig.Assets, ", ")
	fmt.Printf("Symbols evaluated: %s\n\n", symbolsEvaluated)
	log.Fatal("Analysis concluded")
}

func getFeatureModel(analysis *featureAnalysis) *FeatureAnalysis {
	if analysis == nil {
		return nil
	}
	features := analysis.features
	combinedFeatures := analysis.combinedFeatures
	featureFrequencies := []FeatureFrequency{}
	for _, f := range features {
		feature := FeatureFrequency{
			Name: f.name,
		}
		featureFrequencies = append(featureFrequencies, feature)
	}
	for featureIndex := range features[0].counts {
		featuresTotal := 0
		for _, f := range features {
			featuresTotal += f.counts[featureIndex]
		}
		for i, f := range features {
			frequency := float64(f.counts[featureIndex]) / float64(featuresTotal)
			frequencies := &featureFrequencies[i].Frequencies
			*frequencies = append(*frequencies, frequency)
		}
	}
	combinations := [][]float64{}
	offset := 0
	for range features {
		columns := []float64{}
		for range features {
			frequency := float64(combinedFeatures[offset].count) / float64(analysis.combinedFeaturesTotal)
			offset++
			columns = append(columns, frequency)
		}
		combinations = append(combinations, columns)
	}
	model := FeatureAnalysis{
		Features: featureFrequencies,
		Combinations: combinations,
	}
	return &model
}