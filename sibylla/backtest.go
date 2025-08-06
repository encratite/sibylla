package sibylla

import (
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/gammazero/deque"
	"gopkg.in/yaml.v3"
)

type BacktestConfiguration struct {
	DateMin SerializableDate `yaml:"dateMin"`
	DateSplit SerializableDate `yaml:"dateSplit"`
	DateMax SerializableDate `yaml:"dateMax"`
	Leverage *float64 `yaml:"leverage"`
	Strategies []BacktestStrategy `yaml:"strategies"`
}

type BacktestStrategy struct {
	Symbol string `yaml:"symbol"`
	Side SerializableSide `yaml:"side"`
	Time SerializableDuration `yaml:"time"`
	HoldingTime int `yaml:"time"`
	Conditions []StrategyCondition `yaml:"conditions"`
}

type StrategyCondition struct {
	Symbol string `yaml:"feature"`
	Feature string `yaml:"feature"`
	Min float64 `yaml:"min"`
	Max float64 `yaml:"max"`
}

type PositionSide int

const (
	SideLong PositionSide = iota
	SideShort
)

type assetPath struct {
	asset Asset
	path string
}

type assetRecords struct {
	asset Asset
	dailyRecords []DailyRecord
	intradayRecords []FeatureRecord
	recordsMap map[time.Time]*FeatureRecord
}

type strategyCondition struct {
	asset assetRecords
	feature featureAccessor
	min float64
	max float64
}

type backtestData struct {
	conditions []strategyCondition
	returns returnsAccessor
	side PositionSide
	optimizeWeekdays bool
	timeOfDay *time.Duration
	equityCurve []equityCurveSample
	returnsSamples []float64
	returnsTimestamps []time.Time
	weekdayReturns [daysPerWeek][]float64
	optimizationReturns [daysPerWeek]deque.Deque[float64]
	bannedDay *time.Weekday
	riskAdjusted float64
	riskAdjustedMin float64
	riskAdjustedRecent float64
	tradesRatio float64
	cumulativeReturn float64
	cumulativeMax float64
	drawdownMax float64
	enabled bool
}

type equityCurveSample struct {
	timestamp time.Time
	cash float64
}

func Backtest(yamlPath string) {
	loadConfiguration()
	loadCurrencies()
	backtestConfig := loadBacktestConfiguration(yamlPath)
	symbolsMap := map[string]struct{}{}
	for _, strategy := range backtestConfig.Strategies {
		symbolsMap[strategy.Symbol] = struct{}{}
		for _, parameter := range strategy.Conditions {
			symbolsMap[parameter.Feature] = struct{}{}
		}
	}
	symbols := []string{}
	for key := range symbolsMap {
		symbols = append(symbols, key)
	}
	assetRecords := getAssetRecords(
		symbols,
		backtestConfig.DateMin,
		backtestConfig.DateMax,
		nil,
		nil,
	)
	backtests := parallelMap(backtestConfig.Strategies, func (strategy BacktestStrategy) backtestData {
		return executeStrategy(strategy, assetRecords, backtestConfig)
	})
	for i, backtest := range backtests {
		fmt.Printf("%d. RAR: %.2f\n", i + 1, backtest.riskAdjusted)
	}
}

func loadBacktestConfiguration(path string) BacktestConfiguration {
	yamlData := readFile(path)
	configuration := new(BacktestConfiguration)
	err := yaml.Unmarshal(yamlData, configuration)
	if err != nil {
		log.Fatal("Failed to unmarshal YAML:", err)
	}
	configuration.validate()
	return *configuration
}

func (c *BacktestConfiguration) validate() {
	if !c.DateMin.Before(c.DateSplit.Time) || !c.DateSplit.Before(c.DateMax.Time) {
		format := "Invalid dates in backtest configuration: DateMin = %s, DateSplit = %s, DateMax = %s"
		log.Fatalf(format, getDateString(c.DateMin.Time), getDateString(c.DateSplit.Time), getDateString(c.DateMin.Time))
	}
	if len(c.Strategies) == 0 {
		log.Fatal("No strategies configured")
	}
	for _, strategy := range c.Strategies {
		strategy.validate()
	}
}

func (s *BacktestStrategy) validate() {
	firstSymbol := s.Conditions[0].Symbol
	if firstSymbol != "" {
		log.Fatalf("The first symbol must be empty, encountered \"%s\" instead", firstSymbol)
	}
	if len(s.Conditions) == 0 {
		log.Fatal("No conditions defined for strategy")
	}
	for i, condition := range s.Conditions {
		first := i == 0
		condition.validate(first)
	}
}

func (s *BacktestStrategy) getStrategyAssets(assets []assetRecords) []assetRecords {
	symbols := []string{s.Symbol}
	for _, condition := range s.Conditions {
		if condition.Symbol != "" {
			symbols = append(symbols, condition.Symbol)
		}
	}
	strategyRecords := []assetRecords{}
	for _, symbol := range symbols {
		records, recordsExist := find(assets, func (records assetRecords) bool {
			return records.asset.Symbol == symbol
		})
		if !recordsExist {
			log.Fatalf("Unable to find records matching symbol: %s", s.Symbol)
		}
		strategyRecords = append(strategyRecords, records)
	}
	return strategyRecords
}

func (s *BacktestStrategy) getConditions(strategyRecords []assetRecords) []strategyCondition {
	conditions := []strategyCondition{}
	accessors := getFeatureAccessors()
	for i, configurationCondition := range s.Conditions {
		asset := strategyRecords[i]
		feature, exists := find(accessors, func (f featureAccessor) bool {
			return f.name == configurationCondition.Feature
		})
		if !exists {
			log.Fatalf("Unable to find a feature accessor corresponding to name \"%s\"", configurationCondition.Feature)
		}
		condition := strategyCondition{
			asset: asset,
			feature: feature,
			min: configurationCondition.Min,
			max: configurationCondition.Max,
		}
		conditions = append(conditions, condition)
	}
	return conditions
}

func (s *BacktestStrategy) getReturnsAccessor() returnsAccessor {
	returnsAccessors := getReturnsAccessors()
	name := fmt.Sprintf("returns%dH", s.HoldingTime)
	returnsAccessor, exists := find(returnsAccessors, func (r returnsAccessor) bool {
		return r.name == name
	})
	if !exists {
		log.Fatalf("Unable to find a returns accessor with the name \"%s\"", name)
	}
	return returnsAccessor
}

func (c *StrategyCondition) validate(first bool) {
	if c.Min < 0.0 || c.Max > 1.0 || c.Min > c.Max {
		log.Fatalf("Invalid min/max values in condition (min = %.2f, max = %.2f)", c.Min, c.Max)
	}
	if !first && c.Symbol == "" {
		log.Fatal("Only the first condition may have an unset symbol")
	}
}

func getAssetRecords(
	symbols []string,
	dateMin SerializableDate,
	dateMax SerializableDate,
	timeMin *SerializableDuration,
	timeMax *SerializableDuration,
) []assetRecords {
	assetPaths := getAssetPaths(symbols)
	start := time.Now()
	assetRecords := parallelMap(assetPaths, func (path assetPath) assetRecords {
		return executeAssetLoader(
			path,
			dateMin,
			dateMax,
			timeMin,
			timeMax,
		)
	})
	delta := time.Since(start)
	fmt.Printf("Loaded archives in %.2f s\n", delta.Seconds())
	return assetRecords
}

func getAssetPaths(symbols []string) []assetPath {
	assetPaths := []assetPath{}
	for _, asset := range *assets {
		fRecords := 1
		if asset.FRecords != nil {
			fRecords = *asset.FRecords
		}
		baseSymbol := asset.Symbol
		for fNumber := 1; fNumber <= fRecords; fNumber++ {
			path := getArchivePath(baseSymbol, fNumber)
			if fNumber >= 2 {
				asset.Symbol = fmt.Sprintf("%s.F%d", baseSymbol, fNumber)
			}
			if len(symbols) > 0 && !contains(symbols, asset.Symbol) {
				continue
			}
			assetPath := assetPath{
				asset: asset,
				path: path,
			}
			assetPaths = append(assetPaths, assetPath)
		}
	}
	return assetPaths
}

func executeAssetLoader(
	assetPath assetPath,
	dateMin SerializableDate,
	dateMax SerializableDate,
	timeMin *SerializableDuration,
	timeMax *SerializableDuration,
) assetRecords {
	archive := readArchive(assetPath.path)
	dailyRecords := []DailyRecord{}
	intradayRecords := []FeatureRecord{}
	recordsMap := map[time.Time]*FeatureRecord{}
	for _, record := range archive.DailyRecords {
		isValid, breakLoop := isValidDate(record.Date, dateMin, dateMax)
		if !isValid {
			if breakLoop {
				break
			} else {
				continue
			}
		}
		dailyRecords = append(dailyRecords, record)
	}
	for _, record := range archive.IntradayRecords {
		isValid, breakLoop := isValidDate(record.Timestamp, dateMin, dateMax)
		if !isValid {
			if breakLoop {
				break
			} else {
				continue
			}
		}
		isValid = isValidTime(record.Timestamp, timeMin, timeMax)
		if !isValid {
			continue
		}
		intradayRecords = append(intradayRecords, record)
		recordsMap[record.Timestamp] = &record
	}
	return assetRecords{
		asset: assetPath.asset,
		dailyRecords: dailyRecords,
		intradayRecords: intradayRecords,
		recordsMap: recordsMap,
	}
}

func isValidDate(timestamp time.Time, dateMin SerializableDate, dateMax SerializableDate) (bool, bool) {
	if timestamp.Before(dateMin.Time) {
		return false, false
	}
	if !timestamp.Before(dateMax.Time) {
		return false, true
	}
	return true, false
}

func isValidTime(timestamp time.Time, timeMin *SerializableDuration, timeMax *SerializableDuration) bool {
	date := getDateFromTime(timestamp)
	timeOfDay := timestamp.Sub(date)
	if timeMin != nil && timeOfDay < timeMin.Duration {
		return false
	}
	if timeMax != nil && timeOfDay > timeMax.Duration {
		return false
	}
	return true
}

func executeStrategy(strategy BacktestStrategy, assets []assetRecords, backtestConfig BacktestConfiguration) backtestData {
	strategyRecords := strategy.getStrategyAssets(assets)
	conditions := strategy.getConditions(strategyRecords)
	tradedAsset := strategyRecords[0]
	intradayRecords := tradedAsset.intradayRecords
	returnsAccessor := strategy.getReturnsAccessor()
	result := backtestData{
		conditions: conditions,
		returns: returnsAccessor,
		side: strategy.Side.PositionSide,
		optimizeWeekdays: false,
		timeOfDay: &strategy.Time.Duration,
		equityCurve: []equityCurveSample{},
		returnsSamples: []float64{},
		weekdayReturns: [daysPerWeek][]float64{},
		optimizationReturns: [daysPerWeek]deque.Deque[float64]{},
		bannedDay: nil,
		cumulativeReturn: 1.0,
		cumulativeMax: 1.0,
		drawdownMax: 0.0,
		enabled: true,
	}
	for i := range intradayRecords {
		record := &intradayRecords[i]
		if !record.hasReturns() {
			continue
		}
		match := true
		for _, condition := range conditions {
			if !condition.match(record) {
				match = false
				break
			}
		}
		if match {
			onConditionMatch(record, &tradedAsset.asset, backtestConfig.Leverage, false, &result)
		}
	}
	result.postProcess(false, backtestConfig.DateMin.Time, backtestConfig.DateMax.Time, intradayRecords)
	return result
}

func onConditionMatch(
	record *FeatureRecord,
	asset *Asset,
	leverage *float64,
	addTimestamp bool,
	result *backtestData,
) {
	if result.timeOfDay != nil {
		timeOfDay := getTimeOfDay(record.Timestamp)
		if timeOfDay != *result.timeOfDay {
			return
		}
	}
	returnsRecord := result.returns.get(record)
	if returnsRecord == nil {
		return
	}
	cash := 0.0
	equityCurve := &result.equityCurve
	length := len(*equityCurve)
	if length > 0 {
		lastSample := &(*equityCurve)[length - 1]
		duration := record.Timestamp.Sub(lastSample.timestamp)
		holdingTime := time.Duration(result.returns.holdingTime) * time.Hour
		if duration < holdingTime {
			return
		}
		cash = lastSample.cash
	}
	notionalValue := float64(returnsRecord.Ticks1) * asset.TickValue
	delta := returnsRecord.Ticks2 - returnsRecord.Ticks1
	returns := getAssetReturns(result.side, record.Timestamp, delta, true, asset)
	percent := returns / notionalValue
	factor := 1.0 + percent
	weekdayIndex := int(record.Timestamp.Weekday()) - 1
	bannedDay := result.bannedDay
	if result.optimizeWeekdays {
		optimizeWeekdays(percent, weekdayIndex, result)
	}
	if bannedDay != nil && record.Timestamp.Weekday() == *bannedDay {
		return
	}
	if leverage != nil {
		returns *= *leverage
	}
	cash += returns
	sample := equityCurveSample{
		timestamp: record.Timestamp,
		cash: cash,
	}
	*equityCurve = append(*equityCurve, sample)
	result.returnsSamples = append(result.returnsSamples, percent)
	if addTimestamp {
		result.returnsTimestamps = append(result.returnsTimestamps, record.Timestamp)
	}
	result.weekdayReturns[weekdayIndex] = append(result.weekdayReturns[weekdayIndex], percent)
	result.cumulativeReturn *= factor
	result.cumulativeMax = max(result.cumulativeMax, result.cumulativeReturn)
	drawdown := 1.0 - result.cumulativeReturn / result.cumulativeMax
	result.drawdownMax = max(result.drawdownMax, drawdown)
}

func (result *backtestData) postProcess(
	setRiskAdjusted bool,
	dateMin time.Time,
	dateMax time.Time,
	intradayRecords []FeatureRecord,
) {
	result.tradesRatio = getTradesRatio(
		dateMin,
		dateMax,
		result.equityCurve,
		intradayRecords,
	)
	if setRiskAdjusted {
		segmentSize := len(result.returnsSamples) / riskAdjustedSegments
		segments := []float64{}
		for j := range riskAdjustedSegments {
			jNext := j + 1
			offset := j * segmentSize
			end := jNext * segmentSize
			if jNext == riskAdjustedSegments {
				end = len(result.returnsSamples)
			}
			samples := result.returnsSamples[offset:end]
			riskAdjusted := getRiskAdjusted(samples)
			segments = append(segments, riskAdjusted)
		}
		result.riskAdjusted = getRiskAdjusted(result.returnsSamples)
		result.riskAdjustedMin = slices.Min(segments)
		result.riskAdjustedRecent = segments[len(segments) - 1]
		result.returnsSamples = nil
	}
}