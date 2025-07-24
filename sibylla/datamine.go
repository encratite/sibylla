package sibylla

import (
	"fmt"
	"log"
	"math"
	"slices"
	"time"

	"github.com/shopspring/decimal"
	"gonum.org/v1/gonum/stat"
	"gopkg.in/yaml.v3"
)

type DataMiningConfiguration struct {
	Assets []string `yaml:"assets"`
	StrategyLimit int `yaml:"strategyLimit"`
	DateMin *SerializableDate `yaml:"dateMin"`
	DateMax *SerializableDate `yaml:"dateMax"`
	TimeMin *SerializableDuration `yaml:"timeMin"`
	TimeMax *SerializableDuration `yaml:"timeMax"`
	Weekdays []SerializableWeekday `yaml:"weekdays"`
	Thresholds [][]float64 `yaml:"thresholds"`
}

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
	riskAdjusted float64
}

type equityCurveSample struct {
	timestamp time.Time
	cash decimal.Decimal
}

func DataMine(yamlPath string) {
	loadConfiguration()
	loadCurrencies()
	miningConfig := loadDataMiningConfiguration(yamlPath)
	assetPaths := []assetPath{}
	for _, asset := range *assets {
		if len(miningConfig.Assets) > 0 && !contains(miningConfig.Assets, asset.Symbol) {
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
			if miningConfig.DateMin != nil && record.Timestamp.Before(miningConfig.DateMin.Time) {
				continue
			}
			if miningConfig.DateMax != nil && !record.Timestamp.Before(miningConfig.DateMax.Time) {
				break
			}
			date := getDateFromTime(record.Timestamp)
			timeOfDay := record.Timestamp.Sub(date)
			if miningConfig.TimeMin != nil && timeOfDay < miningConfig.TimeMin.Duration {
				continue
			}
			if miningConfig.TimeMax != nil && timeOfDay > miningConfig.TimeMax.Duration {
				continue
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
	start = time.Now()
	tasks := getDataMiningTasks(assetRecords, miningConfig)
	fmt.Printf("Data mining with %d tasks\n", len(tasks))
	taskResults := parallelMap(tasks, executeDataMiningTask)
	delta = time.Since(start)
	fmt.Printf("Finished data mining in %.2f s\n", delta.Seconds())
	assetResults := map[string][]dataMiningResult{}
	for _, results := range taskResults {
		for _, result := range results {
			key := result.task[0].asset.asset.Symbol
			assetResults[key] = append(assetResults[key], result)
		}
	}
	for symbol := range assetResults {
		slices.SortFunc(assetResults[symbol], func (a, b dataMiningResult) int {
			return compareFloat64(b.riskAdjusted, a.riskAdjusted)
		})
		results := assetResults[symbol]
		if len(results) > miningConfig.StrategyLimit {
			results = results[:miningConfig.StrategyLimit]
		}
		assetResults[symbol] = results
	}
}

func getDataMiningTasks(assetRecords []assetRecords, miningConfig DataMiningConfiguration) []dataMiningTask {
	accessors := getFeatureAccessors()
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
					for _, minMax1 := range miningConfig.Thresholds {
						for _, minMax2 := range miningConfig.Thresholds {
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
	allReturnsSamples := [][]float64{}
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
			allReturnsSamples = append(allReturnsSamples, []float64{})
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
			returnsSamples := &allReturnsSamples[i]
			returnsRecord := result.returns.get(record1)
			if returnsRecord == nil {
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
			returns := getAssetReturns(result.side, record1.Timestamp, returnsRecord.Ticks, asset)
			cash = cash.Add(returns)
			sample := equityCurveSample{
				timestamp: record1.Timestamp,
				cash: cash,
			}
			*equityCurve = append(*equityCurve, sample)
			if !math.IsNaN(returnsRecord.Percent) {
				*returnsSamples = append(*returnsSamples, returnsRecord.Percent)
			}
		}
	}
	for i := range results {
		result := &results[i]
		returnsSamples := allReturnsSamples[i]
		mean := stat.Mean(returnsSamples, nil)
		stdDev := stat.StdDev(returnsSamples, nil)
		riskAdjusted := mean / stdDev
		result.riskAdjusted = riskAdjusted
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

func loadDataMiningConfiguration(path string) DataMiningConfiguration {
	yamlData := readFile(path)
	configuration := new(DataMiningConfiguration)
	err := yaml.Unmarshal(yamlData, configuration)
	if err != nil {
		log.Fatal("Failed to unmarshal YAML:", err)
	}
	configuration.sanityCheck()
	return *configuration
}

func (c *DataMiningConfiguration) sanityCheck() {
	if len(c.Thresholds) == 0 {
		log.Fatal("No data mining thresholds were specified")
	}
	if c.DateMin != nil && c.DateMax != nil && !c.DateMin.Before(c.DateMax.Time) {
		format := "Invalid dateMin/dateMax values in data mining configuration: %s vs. %s"
		log.Fatalf(format, getDateString(c.DateMin.Time), getDateString(c.DateMax.Time))
	}
	if c.TimeMin != nil && c.TimeMax != nil && c.TimeMin.Duration > c.TimeMax.Duration {
		format := "Invalid timeMin/timeMax values in data mining configuration: %s vs. %s"
		log.Fatalf(format, *c.TimeMin, *c.TimeMax)
	}
}