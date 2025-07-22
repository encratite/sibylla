package sibylla

import (
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
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

type assetRecords struct {
	asset Asset
	records []FeatureRecord
	recordsMap map[time.Time]*FeatureRecord
}

type featureThreshold struct {
	asset assetRecords
	feature featureAccessor
	min float64
	max float64
}

type dataMiningTask [2]featureThreshold

type dataMiningResult struct {
	task dataMiningTask
	returns returnsAccessor
	side positionSide
	equityCurve []equityCurveSample
}

type equityCurveSample struct {
	timestamp time.Time
	cash decimal.Decimal
}

func DataMine(fromString, toString, assetsString string) {
	loadConfiguration()
	loadCurrencies()
	from := getDateFromArg(fromString)
	to := getDateFromArg(toString)
	assetSymbols := strings.Split(assetsString, " ")
	assetPaths := []assetPath{}
	for _, asset := range *assets {
		if len(assetSymbols) > 0 && !contains(assetSymbols, asset.Symbol) {
			continue
		}
		fRecords := 1
		if asset.FRecords != nil {
			fRecords = *asset.FRecords
		}
		for fNumber := 1; fNumber <= fRecords; fNumber++ {
			path := getArchivePath(asset.Symbol, fNumber)
			assetPath := assetPath{
				asset: asset,
				path: path,
			}
			assetPaths = append(assetPaths, assetPath)
		}
	}
	start := time.Now()
	assetRecords := parallelMap(assetPaths, func (assetPath assetPath) assetRecords {
		archive := readArchive(assetPath.path)
		records := []FeatureRecord{}
		recordsMap := map[time.Time]*FeatureRecord{}
		for _, record := range archive.IntradayRecords {
			if from != nil && record.Timestamp.Before(*from) {
				continue
			}
			if to != nil && !record.Timestamp.Before(*to) {
				break
			}
			records = append(records, record)
			recordsMap[record.Timestamp] = &record
		}
		return assetRecords{
			asset: assetPath.asset,
			records: records,
			recordsMap: recordsMap,
		}
	})
	delta := time.Since(start)
	fmt.Printf("Loaded archives in %.2f s\n", delta.Seconds())
	tasks := getDataMiningTasks(assetRecords)
	results := parallelMap(tasks, executeDataMiningTask)
	fmt.Printf("Results: %d\n", len(results))
}

func getDataMiningTasks(assetRecords []assetRecords) []dataMiningTask {
	accessors := getFeatureAccessors()
	minMax := [][]float64{
		{0.0, 0.3},
		{0.35, 0.65},
		{0.7, 1.0},
	}
	tasks := []dataMiningTask{}
	for i, asset1 := range assetRecords {
		if asset1.asset.FeaturesOnly {
			continue
		}
		for j, asset2 := range assetRecords {
			for k, feature1 := range accessors {
				for l, feature2 := range accessors {
					if i == j && k == l {
						continue
					}
					for _, minMax1 := range minMax {
						for _, minMax2 := range minMax {
							threshold1 := newDataMiningThreshold(asset1, feature1, minMax1)
							threshold2 := newDataMiningThreshold(asset2, feature2, minMax2)
							task := dataMiningTask{threshold1, threshold2}
							tasks = append(tasks, task)
						}
					}
				}
			}
		}
	}
	return tasks
}

func getDateFromArg(argument string) *time.Time {
	if argument != "" {
		date := getDate(argument)
		return &date
	} else {
		return nil
	}
}

func newDataMiningThreshold(asset assetRecords, feature featureAccessor, minMax []float64) featureThreshold {
	return featureThreshold{
		asset: asset,
		feature: feature,
		min: minMax[0],
		max: minMax[1],
	}
}

func executeDataMiningTask(task dataMiningTask) []dataMiningResult {
	threshold1 := &task[0]
	threshold2 := &task[1]
	returnsAccessors := getReturnsAccessors()
	results := []dataMiningResult{}
	sides := [2]positionSide{
		SideLong,
		SideShort,
	}
	for _, returns := range returnsAccessors {
		for _, side := range sides {
			result := dataMiningResult{
				task: task,
				returns: returns,
				side: side,
				equityCurve: []equityCurveSample{},
			}
			results = append(results, result)
		}
	}
	for i := range threshold1.asset.records {
		record1 := &threshold1.asset.records[i]
		if !record1.hasReturns() || !threshold1.match(record1) {
			continue
		}
		record2, exists := threshold2.asset.recordsMap[record1.Timestamp]
		if !exists || !threshold2.match(record2) {
			continue
		}
		asset := &threshold1.asset.asset
		for i := range results {
			result := &results[i]
			ticks := result.returns.get(record1)
			if ticks == nil {
				continue
			}
			cash := decimal.Zero
			equityCurve := &result.equityCurve
			length := len(*equityCurve)
			if length > 0 {
				lastSample := &(*equityCurve)[length - 1]
				duration := record1.Timestamp.Sub(lastSample.timestamp)
				holdingTime := time.Duration(result.returns.holdingTime) * time.Hour
				if duration < holdingTime {
					continue
				}
				cash = lastSample.cash
			}
			returns := getAssetReturns(result.side, record1.Timestamp, *ticks, asset)
			cash = cash.Add(returns)
			sample := equityCurveSample{
				timestamp: record1.Timestamp,
				cash: cash,
			}
			*equityCurve = append(*equityCurve, sample)
		}
	}
	return results
}

func getAssetReturns(side positionSide, timestamp time.Time, ticks int, asset *Asset) decimal.Decimal {
	if side == SideLong {
		ticks -= asset.Spread
	} else {
		ticks += asset.Spread
	}
	ticksDecimal := decimal.NewFromInt(int64(ticks))
	rawGains := ticksDecimal.Mul(asset.TickValue.Decimal)
	gains := convertCurrency(timestamp, rawGains, asset.Currency)
	if side == SideShort {
		gains = gains.Neg()
	}
	fees := asset.BrokerFee.Decimal.Add(asset.ExchangeFee.Decimal)
	gains = gains.Sub(fees)
	return gains
}

func (c *featureThreshold) match(record *FeatureRecord) bool {
	pointer := c.feature.get(record)
	if pointer == nil {
		return false
	}
	value := *pointer
	match := value >= c.min && value <= c.max
	return match
}