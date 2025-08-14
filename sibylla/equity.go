package sibylla

import (
	"log"
	"math"
	"strconv"
	"time"

	"gonum.org/v1/gonum/stat"
)

var riskFreeRate map[monthlyEquityKey]float64

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
	maxCash float64
	maxDrawdown float64
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
		maxCash: initialCash,
		maxDrawdown: 0.0,
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
	d.maxCash = max(d.maxCash, cash)
	drawdown := 1.0 - cash / d.maxCash
	d.maxDrawdown = max(d.maxDrawdown, drawdown)
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

func (d *equityCurveData) getFilterReturns() float64 {
	if !d.empty() {
		first := d.samples[0]
		last := d.samples[len(d.samples) - 1]
		returns := last.cash / first.cash
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

func (d *equityCurveData) getSharpe(
	dateMin time.Time,
	dateMax time.Time,
) float64 {
	performance := d.getPerformance(dateMin, dateMax)
	riskFreeRateSamples := []float64{}
	for date := dateMin; date.Before(dateMax); date = date.AddDate(0, 1, 0) {
		key := newMonthlyEquityKey(date)
		rate, exists := riskFreeRate[key]
		if !exists {
			log.Fatalf("Unable to find a risk free rate sample for %s", getDateString(date))
		}
		riskFreeRateSamples = append(riskFreeRateSamples, rate)
	}
	annualRate := stat.Mean(riskFreeRateSamples, nil) / 100.0
	monthlyRate := math.Pow(1.0 + annualRate, 1.0 / monthsPerYear) - 1.0
	sharpeRatio := (stat.Mean(performance, nil) - monthlyRate) / stat.StdDev(performance, nil)
	annualizedSharpe := math.Sqrt(monthsPerYear) * sharpeRatio
	return annualizedSharpe
}

func (d *equityCurveData) reset() {
	d.samples = nil
	d.endOfMonthCash = nil
}

func loadRiskFreeRate() {
	if riskFreeRate != nil {
		return
	}
	columns := []string{
		"observation_date",
		"TB3MS",
	}
	riskFreeRate = map[monthlyEquityKey]float64{}
	readCsv(configuration.RiskFreeRatePath, columns, func (values []string) {
		date := getDate(values[0])
		key := newMonthlyEquityKey(date)
		rateString := values[1]
		rate, err := strconv.ParseFloat(rateString, 64)
		if err != nil {
			log.Fatalf("Failed to parse rate value \"%s\": %v", rateString, err)
		}
		riskFreeRate[key] = rate
	})
}