package sibylla

import (
	"cmp"
	"fmt"
	"log"
	"math"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"gonum.org/v1/gonum/stat"
)

const recentYears = 2

type segmentedReturnsStats struct {
	index int
	returns float64
	maxDrawdown float64
	sharpe float64
}

type collectedSegmentStats struct {
	isStats []segmentedReturnsStats
	recentStats []segmentedReturnsStats
	oosStats []segmentedReturnsStats
}

type correlationFeature struct {
	name string
	coefficient float64
}

type correlationProperty struct {
	get func (segmentedReturnsStats) float64
	ascending bool
}

func OOSCorrelation(yamlPath string) {
	loadConfiguration()
	loadCurrencies()
	miningConfig := loadDataMiningConfiguration(yamlPath)
	taskResults, _ := executeDataMiningConfig(miningConfig)
	runtime.GC()
	debug.FreeOSMemory()
	splits := miningConfig.CorrelationSplits
	var strategyCount int
	start := time.Now()
	indexes := []int{}
	for i := range splits[:len(splits) - 1] {
		indexes = append(indexes, i)
	}
	periods := len(indexes)
	parallelStats := parallelMap(indexes, func (i int) collectedSegmentStats {
		start := splits[i].Time
		end := splits[i + 1].Time
		output := processOOSSegment(start, end, &strategyCount, taskResults, miningConfig)
		return output
	})
	stats := mergeParallelStats(parallelStats)
	delta := time.Since(start)
	fmt.Printf("Calculated IS/OOS segments in %.2f s\n", delta.Seconds())
	getReturns := correlationProperty{
		get: func (s segmentedReturnsStats) float64 {
			return s.returns
		},
		ascending: false,
	}
	getMaxDrawdown := correlationProperty{
		get: func (s segmentedReturnsStats) float64 {
			return s.maxDrawdown
		},
		ascending: true,
	}
	getSharpeRatio := correlationProperty{
		get: func (s segmentedReturnsStats) float64 {
			return s.sharpe
		},
		ascending: false,
	}
	start = time.Now()
	isStats := stats.isStats
	recentStats := stats.recentStats
	oosStats := stats.oosStats
	features := []correlationFeature{
		getCorrelationFeature("Returns (IS)", isStats, isStats, oosStats, getReturns, miningConfig),
		getCorrelationFeature("Max Drawdown (IS)", isStats, isStats, oosStats, getMaxDrawdown, miningConfig),
		getCorrelationFeature("Sharpe (IS)", isStats, isStats, oosStats, getSharpeRatio, miningConfig),
		getCorrelationFeature("Returns (recent)", recentStats, isStats, oosStats, getReturns, miningConfig),
		getCorrelationFeature("Max Drawdown (recent)", recentStats, isStats, oosStats, getMaxDrawdown, miningConfig),
		getCorrelationFeature("Sharpe (recent)", recentStats, isStats, oosStats, getSharpeRatio, miningConfig),
	}
	slices.SortFunc(features, func (a, b correlationFeature) int {
		return cmp.Compare(math.Abs(b.coefficient), math.Abs(a.coefficient))
	})
	delta = time.Since(start)
	fmt.Printf("Correlated metrics in %.2f s\n", delta.Seconds())
	fmt.Printf("\nConfiguration:\n\n")
	fmt.Printf("\tBacktested period: from %s to %s\n", getDateString(miningConfig.DateMin.Time), getDateString(miningConfig.DateMax.Time))
	firstDate := splits[0].Time
	lastDate := splits[len(splits) - 1].Time
	fmt.Printf("\tNumber of IS/OOS periods evaluated: %d periods\n", periods)
	fmt.Printf("\tRange of IS/OOS splits: from %s to %s\n", getDateString(firstDate), getDateString(lastDate))
	strategyPercent := 100.0 * *miningConfig.StrategyRatio
	fmt.Printf("\tNumber of strategies evaluated per period: top %.2f%% out of %d\n", strategyPercent, strategyCount)
	fmt.Printf("\tNumber of samples used for correlation: %d\n", len(isStats))
	fmt.Printf("\tRange of \"most recent\" data in each IS period: %d years\n", recentYears)
	var featureMode string
	if miningConfig.SingleFeature {
		featureMode = "single feature"
	} else {
		featureMode = "two features"
	}
	fmt.Printf("\tFeature mode: %s\n", featureMode)
	fmt.Printf("\tAssets evaluated: %s\n", strings.Join(miningConfig.Assets, ", "))
	fmt.Printf("\tQuantile range: %.2f (increments of %.4f)\n", miningConfig.Conditions.Range, miningConfig.Conditions.Increment)
	fmt.Printf("\nBest predictors of OOS RAR:\n\n")
	for i, feature := range features {
		fmt.Printf("\t%d. %s: %.3f\n", i + 1, feature.name, feature.coefficient)
	}
	fmt.Println("")
}

func processOOSSegment(
	start time.Time,
	end time.Time,
	strategyCount *int,
	taskResults [][]backtestData,
	miningConfig DataMiningConfiguration,
) collectedSegmentStats {
	stats := collectedSegmentStats{}
	if !miningConfig.DateMin.Before(start) || !start.Before(end) {
		log.Fatalf("Invalid parameters in processOOSSegment (DateMin = %s, start = %s, end = %s)", getDateString(miningConfig.DateMin.Time), getDateString(start), getDateString(end))
	}
	index := 0
	for _, backtests := range taskResults {
		for _, backtest := range backtests {
			if !backtest.enabled {
				continue
			}
			recentTime := start.AddDate(-recentYears, 0, 0)
			isStats, isStatsExist := addSegmentedReturnsStats(
				miningConfig.DateMin.Time,
				start,
				index,
				backtest,
			)
			if !isStatsExist {
				continue
			}
			recentStats, recentStatsExist := addSegmentedReturnsStats(
				recentTime,
				start,
				index,
				backtest,
			)
			if !recentStatsExist {
				continue
			}
			oosStats, oosStatsExist := addSegmentedReturnsStats(
				start,
				end,
				index,
				backtest,
			)
			if !oosStatsExist {
				continue
			}
			stats.isStats = append(stats.isStats, isStats)
			stats.recentStats = append(stats.recentStats, recentStats)
			stats.oosStats = append(stats.oosStats, oosStats)
			index++
		}
	}
	*strategyCount = index
	return stats
}

func addSegmentedReturnsStats(
	start time.Time,
	end time.Time,
	index int,
	backtest backtestData,
) (segmentedReturnsStats, bool) {
	equityCurve := backtest.equityCurve
	returns := equityCurve.getReturns(start, end)
	maxDrawdown := equityCurve.getMaxDrawdown(start, end)
	sharpe := equityCurve.getSharpe(start, end)
	if math.IsNaN(returns) || math.IsNaN(maxDrawdown) || math.IsNaN(sharpe) {
		log.Fatal("Encountered NaN value")
	}
	stats := segmentedReturnsStats{
		index: index,
		returns: returns,
		maxDrawdown: maxDrawdown,
		sharpe: sharpe,
	}
	return stats, true
}

func getCorrelationFeature(
	name string,
	stats []segmentedReturnsStats,
	isStats []segmentedReturnsStats,
	oosStats []segmentedReturnsStats,
	property correlationProperty,
	miningConfig DataMiningConfiguration,
) correlationFeature {
	get := property.get
	sortedStats := make([]segmentedReturnsStats, len(stats))
	copy(sortedStats, stats)
	slices.SortFunc(sortedStats, func (a, b segmentedReturnsStats) int {
		output := cmp.Compare(get(a), get(b))
		if property.ascending {
			output = -output
		}
		return output
	})
	x := []float64{}
	y := []float64{}
	strategyLimit := int(*miningConfig.StrategyRatio * float64(len(sortedStats)))
	for i := range sortedStats {
		if len(x) >= strategyLimit {
			break
		}
		sample := sortedStats[i]
		if isStats[sample.index].maxDrawdown > miningConfig.Drawdown {
			continue
		}
		oosSample := oosStats[sample.index]
		xValue := get(sample)
		yValue := oosSample.sharpe
		x = append(x, xValue)
		y = append(y, yValue)
	}
	coefficient := stat.Correlation(x, y, nil)
	output := correlationFeature{
		name: name,
		coefficient: coefficient,
	}
	return output
}

func mergeParallelStats(parallelStats []collectedSegmentStats) collectedSegmentStats {
	stats := collectedSegmentStats{}
	for i := range parallelStats {
		s := &parallelStats[i]
		stats.isStats = append(stats.isStats, s.isStats...)
		stats.recentStats = append(stats.recentStats, s.recentStats...)
		stats.oosStats = append(stats.oosStats, s.oosStats...)
		s.isStats = nil
		s.recentStats = nil
		s.oosStats = nil
	}
	return stats
}