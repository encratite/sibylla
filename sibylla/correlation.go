package sibylla

import (
	"cmp"
	"fmt"
	"log"
	"math"
	"slices"
	"time"

	"gonum.org/v1/gonum/stat"
)

const recentYears = 2

type segmentedReturnsStats struct {
	returns float64
	maxDrawdown float64
	riskAdjusted float64
}

type correlationFeature struct {
	name string
	coefficient float64
}

func OOSCorrelation(yamlPath string) {
	loadConfiguration()
	loadCurrencies()
	miningConfig := loadDataMiningConfiguration(yamlPath)
	taskResults, _ := executeDataMiningConfig(miningConfig)
	splits := miningConfig.CorrelationSplits
	isStats := []segmentedReturnsStats{}
	recentStats := []segmentedReturnsStats{}
	oosStats := []segmentedReturnsStats{}
	for i := range splits[:len(splits) - 1] {
		start := splits[i].Time
		end := splits[i + 1].Time
		processOOSSegment(start, end, taskResults, miningConfig, &isStats, &recentStats, &oosStats)
	}
	getReturns := func (s segmentedReturnsStats) float64 {
		return s.returns
	}
	getMaxDrawdown := func (s segmentedReturnsStats) float64 {
		return s.maxDrawdown
	}
	getRiskAdjusted := func (s segmentedReturnsStats) float64 {
		return s.riskAdjusted
	}
	features := []correlationFeature{
		getCorrelationFeature("Returns (IS)", isStats, oosStats, getReturns),
		getCorrelationFeature("Max Drawdown (IS)", isStats, oosStats, getMaxDrawdown),
		getCorrelationFeature("RAR (IS)", isStats, oosStats, getRiskAdjusted),
		getCorrelationFeature("Returns (recent)", recentStats, oosStats, getReturns),
		getCorrelationFeature("Max Drawdown (recent)", recentStats, oosStats, getMaxDrawdown),
		getCorrelationFeature("RAR (recent)", recentStats, oosStats, getRiskAdjusted),
	}
	slices.SortFunc(features, func (a, b correlationFeature) int {
		return cmp.Compare(math.Abs(b.coefficient), math.Abs(a.coefficient))
	})
	fmt.Printf("\nBest predictors of OOS RAR (%d samples, %d years of recent IS data):\n\n", len(isStats), recentYears)
	for i, feature := range features {
		fmt.Printf("\t%d. %s: %.3f\n", i + 1, feature.name, feature.coefficient)
	}
	fmt.Println("")
}

func processOOSSegment(
	start time.Time,
	end time.Time,
	taskResults [][]dataMiningResult,
	miningConfig DataMiningConfiguration,
	isStats *[]segmentedReturnsStats,
	recentStats *[]segmentedReturnsStats,
	oosStats *[]segmentedReturnsStats,
) {
	if !miningConfig.DateMin.Before(start) || !start.Before(end) {
		log.Fatalf("Invalid parameters in processOOSSegment (DateMin = %s, start = %s, end = %s)", getDateString(miningConfig.DateMin.Time), getDateString(start), getDateString(end))
	}
	for _, results := range taskResults {
		for _, result := range results {
			if !result.enabled {
				continue
			}
			recentTime := start.AddDate(-recentYears, 0, 0)
			addSegmentedReturnsStats(miningConfig.DateMin.Time, start, result.returnsSamples, result.returnsTimestamps, isStats)
			addSegmentedReturnsStats(recentTime, start, result.returnsSamples, result.returnsTimestamps, recentStats)
			addSegmentedReturnsStats(start, end, result.returnsSamples, result.returnsTimestamps, oosStats)
		}
	}
}

func addSegmentedReturnsStats(
	start time.Time,
	end time.Time,
	returnsSamples []float64,
	returnsTimestamps []time.Time,
	output *[]segmentedReturnsStats,
) {
	matchingSamples := getReturnsSamples(start, end, returnsSamples, returnsTimestamps)
	returns, maxDrawdown := getReturnsDrawdown(matchingSamples)
	riskAdjusted := getRiskAdjusted(matchingSamples)
	stats := segmentedReturnsStats{
		returns: returns,
		maxDrawdown: maxDrawdown,
		riskAdjusted: riskAdjusted,
	}
	*output = append(*output, stats)
}

func getReturnsSamples(start time.Time, end time.Time, returnsSamples []float64, returnsTimestamps []time.Time) []float64 {
	if !start.Before(end) {
		log.Fatalf("Invalid parameters in getReturnsSamples (start = %s, end = %s)", getDateString(start), getDateString(end))
	}
	output := []float64{}
	for i := range returnsSamples {
		t := returnsTimestamps[i]
		if !start.After(t) && t.Before(end) {
			output = append(output, returnsSamples[i])
		}
	}
	return output
}

func getReturnsDrawdown(returnsSamples []float64) (float64, float64) {
	returns := 1.0
	maxReturns := returns
	maxDrawdown := 0.0
	for _, x := range returnsSamples {
		returns *= 1.0 + x
		maxReturns = max(maxReturns, returns)
		drawdown := 1.0 - returns / maxReturns
		maxDrawdown = max(maxDrawdown, drawdown)
	}
	return returns, maxDrawdown
}

func getCorrelationFeature(name string, stats []segmentedReturnsStats, oosStats []segmentedReturnsStats, get func (segmentedReturnsStats) float64) correlationFeature {
	x := []float64{}
	y := []float64{}
	for i := range stats {
		x = append(x, get(stats[i]))
		y = append(y, oosStats[i].riskAdjusted)
	}
	coefficient := stat.Correlation(x, y, nil)
	output := correlationFeature{
		name: name,
		coefficient: coefficient,
	}
	return output
}