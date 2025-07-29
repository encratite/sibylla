package sibylla

import (
	"fmt"
	"log"

	"gonum.org/v1/gonum/stat"
)

const enableWeekdayAnalysis = false

type weekdayOptimizationCategory struct {
	category string
	notOptimized weekdayOptimizationStats
	optimized weekdayOptimizationStats
}

type weekdayOptimizationStats struct {
	description string
	riskAdjusted []float64
	riskAdjustedMin []float64
	riskAdjustedRecent []float64
}

func analyzeWeekdayOptimizations(assetResults map[string][]dataMiningResult) {
	all := newWeekdayCategory("All")
	categories := []weekdayOptimizationCategory{}
	for symbol, results := range assetResults {
		category := newWeekdayCategory(symbol)
		for _, result := range results {
			if result.optimizeWeekdays {
				all.optimized.submit(&result)
				category.optimized.submit(&result)
			} else {
				all.notOptimized.submit(&result)
				category.notOptimized.submit(&result)
			}
		}
		categories = append(categories, category)
	}
	fmt.Printf("Optimization buffer size: %d\n", weekdayOptimizationBuffer)
	all.print()
	for _, category := range categories {
		category.print()
	}
	log.Fatal("Analysis concluded")
}

func newWeekdayCategory(category string) weekdayOptimizationCategory {
	notOptimized := weekdayOptimizationStats{
		description: "Not optimized",
	}
	optimized := weekdayOptimizationStats{
		description: "Optimized",
	}
	pair := weekdayOptimizationCategory{
		category: category,
		notOptimized: notOptimized,
		optimized: optimized,
	}
	return pair
}

func (w *weekdayOptimizationCategory) print() {
	w.notOptimized.print(w.category)
	w.optimized.print(w.category)
}

func (w *weekdayOptimizationStats) submit(result *dataMiningResult) {
	w.riskAdjusted = append(w.riskAdjusted, result.riskAdjusted)
	w.riskAdjustedMin = append(w.riskAdjustedMin, result.riskAdjustedMin)
	w.riskAdjustedRecent = append(w.riskAdjustedRecent, result.riskAdjustedRecent)
}

func (w *weekdayOptimizationStats) print(category string) {
	riskAdjustedMean := stat.Mean(w.riskAdjusted, nil)
	riskAdjustedMinMean := stat.Mean(w.riskAdjustedMin, nil)
	riskAdjustedRecentMean := stat.Mean(w.riskAdjustedRecent, nil)
	fmt.Printf("[%s] %s:\n\tmean RAR = %.5f, mean MinRAR = %.5f, mean RecRAR = %.5f\n", category, w.description, riskAdjustedMean, riskAdjustedMinMean, riskAdjustedRecentMean)
}