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
	HoldingTime int `yaml:"holdingTime"`
	Conditions []StrategyCondition `yaml:"conditions"`
}

type StrategyCondition struct {
	Symbol string `yaml:"symbol"`
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

type backtestComparison struct {
	isBacktest backtestData
	oosBacktest backtestData
	completeBacktest backtestData
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
	start := time.Now()
	comparisons := parallelMap(backtestConfig.Strategies, func (strategy BacktestStrategy) backtestComparison {
		return executeStrategy(strategy, assetRecords, backtestConfig)
	})
	delta := time.Since(start)
	fmt.Printf("Performed backtests in %.2f s\n", delta.Seconds())
	fmt.Printf("\nIS period: %s to %s\n", getDateString(backtestConfig.DateMin.Time), getDateString(backtestConfig.DateSplit.Time))
	fmt.Printf("OOS period: %s to %s\n", getDateString(backtestConfig.DateSplit.Time), getDateString(backtestConfig.DateMax.Time))
	fmt.Printf("Number of strategies: %d\n\n", len(backtestConfig.Strategies))
	for i, comparison := range comparisons {
		backtest := comparison.completeBacktest
		condition := backtest.conditions[0]
		symbol := condition.asset.asset.Symbol
		feature := condition.feature.name
		side := "long"
		if backtest.side == SideShort {
			side = "short"
		}
		format := "%d. %s.%s (%.2f, %.2f), %s, %s, %dh\n"
		fmt.Printf(
			format,
			i + 1,
			symbol,
			feature,
			condition.min,
			condition.max,
			side,
			getTimeOfDayString(*backtest.timeOfDay),
			backtest.returns.holdingTime,
		)
		fmt.Printf("\tIS RAR:    %.3f\n", comparison.isBacktest.riskAdjusted)
		fmt.Printf("\tIS RecRAR: %.3f\n", comparison.isBacktest.riskAdjustedRecent)
		fmt.Printf("\tOOS RAR:   %.3f\n", comparison.oosBacktest.riskAdjusted)
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

func executeStrategy(strategy BacktestStrategy, assets []assetRecords, backtestConfig BacktestConfiguration) backtestComparison {
	strategyRecords := strategy.getStrategyAssets(assets)
	conditions := strategy.getConditions(strategyRecords)
	tradedAsset := strategyRecords[0]
	intradayRecords := tradedAsset.intradayRecords
	returnsAccessor := strategy.getReturnsAccessor()
	perform := func (dateMin, dateMax time.Time) backtestData {
		return performBacktest(
			dateMin,
			dateMax,
			returnsAccessor,
			intradayRecords,
			tradedAsset,
			conditions,
			strategy,
			backtestConfig,
		)
	}
	isBacktest := perform(backtestConfig.DateMin.Time, backtestConfig.DateSplit.Time)
	oosBacktest := perform(backtestConfig.DateSplit.Time, backtestConfig.DateMax.Time)
	completeBacktest := perform(backtestConfig.DateMin.Time, backtestConfig.DateMax.Time)
	output := backtestComparison{
		isBacktest: isBacktest,
		oosBacktest: oosBacktest,
		completeBacktest: completeBacktest,
	}
	return output
}

func performBacktest(
	dateMin time.Time,
	dateMax time.Time,
	returns returnsAccessor,
	intradayRecords []FeatureRecord,
	tradedAsset assetRecords,
	conditions []strategyCondition,
	strategy BacktestStrategy,
	backtestConfig BacktestConfiguration,
) backtestData {
	backtest := newBacktest(strategy.Side.PositionSide, &strategy.Time.Duration, conditions, returns)
	for i := range intradayRecords {
		record := &intradayRecords[i]
		if record.Timestamp.Before(dateMin) || !record.Timestamp.Before(dateMax) {
			continue
		}
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
			onConditionMatch(record, &tradedAsset.asset, backtestConfig.Leverage, false, &backtest)
		}
	}
	backtest.postProcess(true, backtestConfig.DateMin.Time, backtestConfig.DateMax.Time, intradayRecords)
	return backtest
}

func onConditionMatch(
	record *FeatureRecord,
	asset *Asset,
	leverage *float64,
	addTimestamp bool,
	backtest *backtestData,
) {
	if backtest.timeOfDay != nil {
		timeOfDay := getTimeOfDay(record.Timestamp)
		if timeOfDay != *backtest.timeOfDay {
			return
		}
	}
	returnsRecord := backtest.returns.get(record)
	if returnsRecord == nil {
		return
	}
	cash := 0.0
	equityCurve := &backtest.equityCurve
	length := len(*equityCurve)
	if length > 0 {
		lastSample := &(*equityCurve)[length - 1]
		duration := record.Timestamp.Sub(lastSample.timestamp)
		holdingTime := time.Duration(backtest.returns.holdingTime) * time.Hour
		if duration < holdingTime {
			return
		}
		cash = lastSample.cash
	}
	notionalValue := float64(returnsRecord.Ticks1) * asset.TickValue
	delta := returnsRecord.Ticks2 - returnsRecord.Ticks1
	returns := getAssetReturns(backtest.side, record.Timestamp, delta, true, asset)
	percent := returns / notionalValue
	factor := 1.0 + percent
	weekdayIndex := int(record.Timestamp.Weekday()) - 1
	bannedDay := backtest.bannedDay
	if backtest.optimizeWeekdays {
		optimizeWeekdays(percent, weekdayIndex, backtest)
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
	backtest.returnsSamples = append(backtest.returnsSamples, percent)
	if addTimestamp {
		backtest.returnsTimestamps = append(backtest.returnsTimestamps, record.Timestamp)
	}
	backtest.weekdayReturns[weekdayIndex] = append(backtest.weekdayReturns[weekdayIndex], percent)
	backtest.cumulativeReturn *= factor
	backtest.cumulativeMax = max(backtest.cumulativeMax, backtest.cumulativeReturn)
	drawdown := 1.0 - backtest.cumulativeReturn / backtest.cumulativeMax
	backtest.drawdownMax = max(backtest.drawdownMax, drawdown)
}

func (backtest *backtestData) postProcess(
	setRiskAdjusted bool,
	dateMin time.Time,
	dateMax time.Time,
	intradayRecords []FeatureRecord,
) {
	backtest.tradesRatio = getTradesRatio(
		dateMin,
		dateMax,
		backtest.equityCurve,
		intradayRecords,
	)
	if setRiskAdjusted {
		segmentSize := len(backtest.returnsSamples) / riskAdjustedSegments
		segments := []float64{}
		for j := range riskAdjustedSegments {
			jNext := j + 1
			offset := j * segmentSize
			end := jNext * segmentSize
			if jNext == riskAdjustedSegments {
				end = len(backtest.returnsSamples)
			}
			samples := backtest.returnsSamples[offset:end]
			riskAdjusted := getRiskAdjusted(samples)
			segments = append(segments, riskAdjusted)
		}
		backtest.riskAdjusted = getRiskAdjusted(backtest.returnsSamples)
		backtest.riskAdjustedMin = slices.Min(segments)
		backtest.riskAdjustedRecent = segments[len(segments) - 1]
		backtest.returnsSamples = nil
	}
}

func newBacktest(
	side PositionSide,
	timeOfDay *time.Duration,
	conditions []strategyCondition,
	returns returnsAccessor,
) backtestData {
	return backtestData{
		conditions: conditions,
		returns: returns,
		side: side,
		optimizeWeekdays: false,
		timeOfDay: timeOfDay,
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
}