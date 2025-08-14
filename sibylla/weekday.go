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
	sharpes []float64
	minSharpes []float64
	recentSharpes []float64
}

func analyzeWeekdayOptimizations(assetResults map[string][]backtestData) {
	if !enableWeekdayAnalysis {
		return
	}
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

func (w *weekdayOptimizationStats) submit(backtest *backtestData) {
	w.sharpes = append(w.sharpes, backtest.sharpe)
	w.minSharpes = append(w.minSharpes, backtest.minSharpe)
	w.recentSharpes = append(w.recentSharpes, backtest.recentSharpe)
}

func (w *weekdayOptimizationStats) print(category string) {
	meanSharpe := stat.Mean(w.sharpes, nil)
	meanMinSharpe := stat.Mean(w.minSharpes, nil)
	meanRecentSharpe := stat.Mean(w.recentSharpes, nil)
	fmt.Printf("[%s] %s:\n\tmean total SR = %.5f, mean min SR = %.5f, mean recent SR = %.5f\n", category, w.description, meanSharpe, meanMinSharpe, meanRecentSharpe)
}