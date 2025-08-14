package sibylla

import (
	"log"
	"math"
	"time"

	"gonum.org/v1/gonum/stat"
)

type equityCurveSample struct {
	timestamp time.Time
	cash float64
}

type monthlyEquityKey struct {
	year int
	month int
}

type equityCurveData struct {
	initialCash float64
	samples []equityCurveSample
	endOfMonthCash map[monthlyEquityKey]float64
}

func newMonthlyEquityKey(timestamp time.Time) monthlyEquityKey {
	return monthlyEquityKey{
		year: timestamp.Year(),
		month: int(timestamp.Month()),
	}
}

func newEquityCurve(initialCash float64) equityCurveData {
	return equityCurveData{
		initialCash: initialCash,
		samples: nil,
		endOfMonthCash: map[monthlyEquityKey]float64{},
	}
}

func (d *equityCurveData) add(timestamp time.Time, cash float64) {
	if d.empty() {
		initialTimestamp := getDateFromTime(timestamp)
		initialSample := equityCurveSample{
			timestamp: initialTimestamp,
			cash: d.initialCash,
		}
		d.samples = append(d.samples, initialSample)
	}
	sample := equityCurveSample{
		timestamp: timestamp,
		cash: cash,
	}
	d.samples = append(d.samples, sample)
	key := newMonthlyEquityKey(timestamp)
	d.endOfMonthCash[key] = cash
}

func (d *equityCurveData) empty() bool {
	return len(d.samples) == 0
}

func (d *equityCurveData) getPerformance(
	dateMin time.Time,
	dateMax time.Time,
) []float64 {
	output := []float64{}
	cash := d.initialCash
	for date := dateMin; date.Before(dateMax); date = date.AddDate(0, 1, 0) {
		key := newMonthlyEquityKey(date)
		newCash, exists := d.endOfMonthCash[key]
		returns := 0.0
		if exists {
			r, valid := getRateOfChange(newCash, cash)
			if valid {
				returns = r
			}
			cash = newCash
		}
		output = append(output, returns)
	}
	return output
}

func (d *equityCurveData) getReturns(
	dateMin time.Time,
	dateMax time.Time,
) float64 {
	if !d.empty() {
		var first *float64
		var last float64
		for _, sample := range d.samples {
			if sample.timestamp.Before(dateMin) {
				continue
			}
			if sample.timestamp.After(dateMax) {
				break
			}
			if first == nil {
				first = &sample.cash
			}
			last = sample.cash
		}
		returns, success := getRateOfChange(last, *first)
		if !success {
			log.Fatal("Failed to calculate returns")
		}
		return returns
	} else {
		return 1.0
	}
}

func (d *equityCurveData) getMaxDrawdown(
	dateMin time.Time,
	dateMax time.Time,
) float64 {
	var maxCash *float64
	maxDrawdown := 0.0
	for _, sample := range d.samples {
		if sample.timestamp.Before(dateMin) {
			continue
		}
		if sample.timestamp.After(dateMax) {
			break
		}
		cash := sample.cash
		if maxCash != nil {
			newMax := max(*maxCash, cash)
			maxCash = &newMax
		} else {
			maxCash = &cash
		}
		drawdown := 1.0 - cash / *maxCash
		maxDrawdown = max(maxDrawdown, drawdown)
	}
	return maxDrawdown
}

func (d *equityCurveData) getRiskAdjusted(
	dateMin time.Time,
	dateMax time.Time,
) float64 {
	performance := d.getPerformance(dateMin, dateMax)
	riskFreeRate := 0.0
	riskAdjusted := (stat.Mean(performance, nil) - riskFreeRate) / stat.StdDev(performance, nil)
	annualized := math.Sqrt(monthsPerYear) * riskAdjusted
	return annualized
}

func (d *equityCurveData) reset() {
	d.samples = nil
	d.endOfMonthCash = nil
}