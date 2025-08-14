package sibylla

import (
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/gammazero/deque"
	"gonum.org/v1/gonum/stat"
	"gopkg.in/yaml.v3"
)

const buyAndHoldSymbol = "ES"
const buyAndHoldTimeOfDay = 12
const stopLossSlippage = 2
const monthsPerYear = 12

type BacktestConfiguration struct {
	DateMin SerializableDate `yaml:"dateMin"`
	DateSplit SerializableDate `yaml:"dateSplit"`
	DateMax SerializableDate `yaml:"dateMax"`
	InitialCash *float64 `yaml:"initialCash"`
	Leverage *float64 `yaml:"leverage"`
	Strategies []BacktestStrategy `yaml:"strategies"`
}

type BacktestStrategy struct {
	Symbol string `yaml:"symbol"`
	Side SerializableSide `yaml:"side"`
	Weekday *SerializableWeekday `yaml:"weekday"`
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
	symbol string
	conditions []strategyCondition
	returns returnsAccessor
	side PositionSide
	optimizeWeekdays bool
	timeOfDay *time.Duration
	equityCurve equityCurveData
	weekdayReturns [daysPerWeek][]float64
	optimizationReturns [daysPerWeek]deque.Deque[float64]
	bannedDay *time.Weekday
	sharpe float64
	minSharpe float64
	recentSharpe float64
	buyAndHoldSharpe float64
	tradesRatio float64
	enabled bool
	seasonalityMode bool
	weekday *time.Weekday
	enableStopLoss bool
	stopLoss *float64
	stopLossHit bool
}

type backtestComparison struct {
	isBacktest backtestData
	oosBacktest backtestData
	completeBacktest backtestData
}

type sharpeRatioData struct {
	sharpeIS []float64
	recentSharpeIS []float64
	sharpeOOS []float64
}

func Backtest(yamlPath string) {
	loadConfiguration()
	loadCurrencies()
	backtestConfig := loadBacktestConfiguration(yamlPath)
	symbolsMap := map[string]struct{}{
		buyAndHoldSymbol: {},
	}
	for _, strategy := range backtestConfig.Strategies {
		symbolsMap[strategy.Symbol] = struct{}{}
		for _, parameter := range strategy.Conditions {
			if parameter.Symbol != "" {
				symbolsMap[parameter.Symbol] = struct{}{}
			}
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
	buyAndHoldEquityCurve := getBuyAndHold(buyAndHoldSymbol, &backtestConfig.DateMin.Time, &backtestConfig.DateMax.Time, assetRecords, *backtestConfig.InitialCash)
	buyAndHoldPerformance := buyAndHoldEquityCurve.getPerformance(backtestConfig.DateMin.Time, backtestConfig.DateMax.Time)
	sharpeRatioData := getSharpeRatioData(comparisons, buyAndHoldPerformance, backtestConfig)
	printStats(sharpeRatioData, assetRecords, backtestConfig)
}

func getSharpeRatioData(comparisons []backtestComparison, buyAndHoldPerformance []float64, backtestConfig BacktestConfiguration) sharpeRatioData {
	sharpeIS := []float64{}
	recentSharpeIS := []float64{}
	sharpeOOS := []float64{}
	for i, comparison := range comparisons {
		backtest := comparison.completeBacktest
		var conditionString string
		if backtest.weekday == nil {
		conditionStrings := []string{}
			for _, condition := range backtest.conditions {
				symbol := condition.asset.asset.Symbol
				feature := condition.feature.name
				min := condition.min
				max := condition.max
				output := fmt.Sprintf("%s.%s (%.2f, %.2f)", symbol, feature, min, max)
				conditionStrings = append(conditionStrings, output)
			}
			conditionString = strings.Join(conditionStrings, ", ")
		} else {
			conditionString = fmt.Sprintf("%s, %s", backtest.symbol, backtest.weekday.String())
		}
		side := "long"
		if backtest.side == SideShort {
			side = "short"
		}
		format := "%d. %s, %s, %s, %dh\n"
		fmt.Printf(
			format,
			i + 1,
			conditionString,
			side,
			getTimeOfDayString(*backtest.timeOfDay),
			backtest.returns.holdingTime,
		)
		performance := comparison.completeBacktest.equityCurve.getPerformance(backtestConfig.DateMin.Time, backtestConfig.DateMax.Time)
		performanceCorrelation := stat.Correlation(performance, buyAndHoldPerformance, nil)
		fmt.Printf("\tIS RAR:              %.3f\n", comparison.isBacktest.sharpe)
		fmt.Printf("\tIS RecRAR:           %.3f\n", comparison.isBacktest.recentSharpe)
		fmt.Printf("\tOOS RAR:             %.3f\n", comparison.oosBacktest.sharpe)
		fmt.Printf("\tMarket correlation:  %.3f\n\n", performanceCorrelation)
		sharpeIS = append(sharpeIS, comparison.isBacktest.sharpe)
		recentSharpeIS = append(recentSharpeIS, comparison.isBacktest.recentSharpe)
		sharpeOOS = append(sharpeOOS, comparison.oosBacktest.sharpe)
	}
	return sharpeRatioData{
		sharpeIS: sharpeIS,
		recentSharpeIS: recentSharpeIS,
		sharpeOOS: sharpeOOS,
	}
}

func printStats(
	sharpeData sharpeRatioData,
	assetRecords []assetRecords,
	backtestConfig BacktestConfiguration,
) {
	strategyCount := len(backtestConfig.Strategies)
	fmt.Printf("IS period: %s to %s\n", getDateString(backtestConfig.DateMin.Time), getDateString(backtestConfig.DateSplit.Time))
	fmt.Printf("OOS period: %s to %s\n", getDateString(backtestConfig.DateSplit.Time), getDateString(backtestConfig.DateMax.Time))
	fmt.Printf("Number of strategies: %d\n\n", strategyCount)
	sharpeCorrelation := stat.Correlation(sharpeData.sharpeIS, sharpeData.sharpeOOS, nil)
	recentSharpeCorrelation := stat.Correlation(sharpeData.recentSharpeIS, sharpeData.sharpeOOS, nil)
	fmt.Printf("PCC(IS Sharpe, OOS Sharpe):        %.3f\n", sharpeCorrelation)
	fmt.Printf("PCC(recent IS Sharpe, OOS Sharpe): %.3f\n\n", recentSharpeCorrelation)
	buyAndHoldReturnsIS := getBuyAndHold(buyAndHoldSymbol, &backtestConfig.DateMin.Time, &backtestConfig.DateSplit.Time, assetRecords, *backtestConfig.InitialCash)
	buyAndHoldReturnsOOS := getBuyAndHold(buyAndHoldSymbol, &backtestConfig.DateSplit.Time, &backtestConfig.DateMax.Time, assetRecords, *backtestConfig.InitialCash)
	buyAndHoldSharpeIS := buyAndHoldReturnsIS.getSharpe(backtestConfig.DateMin.Time, backtestConfig.DateSplit.Time)
	buyAndHoldSharpeOOS := buyAndHoldReturnsOOS.getSharpe(backtestConfig.DateSplit.Time, backtestConfig.DateMax.Time)
	fmt.Printf("Buy and Hold IS Sharpe:  %.3f\n", buyAndHoldSharpeIS)
	fmt.Printf("Buy and Hold OOS Sharpe: %.3f\n\n", buyAndHoldSharpeOOS)
	meanSharpeIS := stat.Mean(sharpeData.sharpeIS, nil)
	meanRecentSharpeIS := stat.Mean(sharpeData.recentSharpeIS, nil)
	meanSharpeOOS := stat.Mean(sharpeData.sharpeOOS, nil)
	fmt.Printf("Mean(IS Sharpe):         %.3f\n", meanSharpeIS)
	fmt.Printf("Mean(recent IS Sharpe):  %.3f\n", meanRecentSharpeIS)
	fmt.Printf("Mean(OOS Sharpe):        %.3f\n\n", meanSharpeOOS)
	printClassifications(buyAndHoldSharpeOOS, sharpeData.sharpeOOS, strategyCount)
}

func printClassifications(
	buyAndHoldSharpeOOS float64,
	sharpeOOS []float64,
	strategyCount int,
) {
	outperform := 0
	underperform := 0
	loss := 0
	for _, sharpe := range sharpeOOS {
		if sharpe > buyAndHoldSharpeOOS {
			outperform++
		} else if sharpe > 0 {
			underperform++
		} else {
			loss++
		}
	}
	outperformPercentage := getPercentageFromInts(outperform, strategyCount)
	underperformPercentage := getPercentageFromInts(underperform, strategyCount)
	lossPercentage := getPercentageFromInts(loss, strategyCount)
	fmt.Printf("OOS performance classifications:\n\n")
	fmt.Printf("\tOutperform:   %.1f%% (%d samples)\n", outperformPercentage, outperform)
	fmt.Printf("\tUnderperform: %.1f%% (%d samples)\n", underperformPercentage, underperform)
	fmt.Printf("\tLoss:         %.1f%% (%d samples)\n\n", lossPercentage, loss)
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
	if c.InitialCash == nil || *c.InitialCash < 1000 {
		log.Fatalf("Invalid initial cash: %.1f", *c.InitialCash)
	}
	if c.Leverage != nil && *c.Leverage <= 0.0 {
		log.Fatalf("Invalid leverage: %.1f", *c.Leverage)
	}
	for _, strategy := range c.Strategies {
		strategy.validate()
	}
}

func (s *BacktestStrategy) validate() {
	if s.Weekday == nil {
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
		isBuyAndHold := record.Timestamp.Hour() == buyAndHoldTimeOfDay
		if !isValid && !isBuyAndHold {
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
	backtest := newBacktest(
		strategy.Symbol,
		strategy.Side.PositionSide,
		&strategy.Time.Duration,
		conditions,
		returns,
		*backtestConfig.InitialCash,
	)
	if strategy.Weekday != nil {
		backtest.weekday = &strategy.Weekday.Weekday
	}
	for i := range intradayRecords {
		record := &intradayRecords[i]
		if record.Timestamp.Before(dateMin) || !record.Timestamp.Before(dateMax) {
			continue
		}
		if !record.hasReturns() {
			continue
		}
		match := true
		if backtest.weekday == nil {
			for _, condition := range conditions {
				conditionRecord, exists := condition.asset.recordsMap[record.Timestamp]
				if !exists || !condition.match(conditionRecord) {
					match = false
					break
				}
			}
		} else {
			weekdayMatch := record.Timestamp.Weekday() == *backtest.weekday
			timeOfDay := getTimeOfDay(record.Timestamp)
			timeOfDayMatch := timeOfDay == *backtest.timeOfDay
			match = weekdayMatch && timeOfDayMatch
		}
		if match {
			onConditionMatch(record, &tradedAsset.asset, backtestConfig.Leverage, &backtest)
		}
	}
	backtest.postProcess(true, backtestConfig.DateMin.Time, backtestConfig.DateMax.Time, intradayRecords)
	return backtest
}

func onConditionMatch(
	record *FeatureRecord,
	asset *Asset,
	leverage *float64,
	backtest *backtestData,
) {
	if !backtest.enabled {
		return
	}
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
	equityCurve := &backtest.equityCurve
	cash := equityCurve.initialCash
	length := len(equityCurve.samples)
	if length > 0 {
		lastSample := &equityCurve.samples[length - 1]
		duration := record.Timestamp.Sub(lastSample.timestamp)
		holdingTime := time.Duration(backtest.returns.holdingTime) * time.Hour
		if duration < holdingTime {
			return
		}
		cash = lastSample.cash
	}
	delta := returnsRecord.Close2 - returnsRecord.Close1
	if backtest.enableStopLoss {
		processStopLoss(&delta,	returnsRecord, backtest)
	}
	returns := getAssetReturns(backtest.side, record.Timestamp, delta, true, asset)
	notionalValue := float64(returnsRecord.Close1) * asset.TickValue
	percent := returns / notionalValue
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
	equityCurve.add(record.Timestamp, cash)
	backtest.weekdayReturns[weekdayIndex] = append(backtest.weekdayReturns[weekdayIndex], percent)
}

func processStopLoss(
	delta *int,
	returnsRecord *ReturnsRecord,
	backtest *backtestData,
) {
	if backtest.side == SideLong {
		drawdown := 1.0 - float64(returnsRecord.Low) / float64(returnsRecord.Close1)
		if drawdown > *backtest.stopLoss {
			stopLossLevel := int((1.0 - *backtest.stopLoss) * float64(returnsRecord.Close1)) - stopLossSlippage
			*delta = stopLossLevel - returnsRecord.Close1
			backtest.stopLossHit = true
		}
	} else {
		drawdown := float64(returnsRecord.High) / float64(returnsRecord.Close1) - 1.0
		if drawdown > *backtest.stopLoss {
			stopLossLevel := int((1.0 + *backtest.stopLoss) * float64(returnsRecord.Close1)) + stopLossSlippage
			*delta = stopLossLevel - returnsRecord.Close1
			backtest.stopLossHit = true
		}
	}
}

func (backtest *backtestData) postProcess(
	setSharpe bool,
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
	if setSharpe {
		sharpeSegments := []float64{}
		delta := dateMax.Sub(dateMin)
		segmentDuration := delta / sharpeSegmentCount
		segmentStart := dateMin
		for range sharpeSegmentCount {
			segmentEnd := segmentStart.Add(segmentDuration)
			sharpeRatio := backtest.equityCurve.getSharpe(segmentStart, segmentEnd)
			sharpeSegments = append(sharpeSegments, sharpeRatio)
			segmentStart = segmentEnd
		}
		backtest.sharpe = backtest.equityCurve.getSharpe(dateMin, dateMax)
		backtest.minSharpe = slices.Min(sharpeSegments)
		backtest.recentSharpe = sharpeSegments[len(sharpeSegments) - 1]
	}
}

func newBacktest(
	symbol string,
	side PositionSide,
	timeOfDay *time.Duration,
	conditions []strategyCondition,
	returns returnsAccessor,
	initialCash float64,
) backtestData {
	return backtestData{
		symbol: symbol,
		conditions: conditions,
		returns: returns,
		side: side,
		optimizeWeekdays: false,
		timeOfDay: timeOfDay,
		equityCurve: newEquityCurve(initialCash),
		weekdayReturns: [daysPerWeek][]float64{},
		optimizationReturns: [daysPerWeek]deque.Deque[float64]{},
		bannedDay: nil,
		enabled: true,
		enableStopLoss: false,
		stopLoss: nil,
		stopLossHit: false,
	}
}

func getBuyAndHold(
	symbol string,
	dateMin *time.Time,
	dateMax *time.Time,
	assets []assetRecords,
	initialCash float64,
) equityCurveData {
	equityCurve := newEquityCurve(initialCash)
	cash := equityCurve.initialCash
	records, exists := find(assets, func (x assetRecords) bool {
		return x.asset.Symbol == symbol
	})
	if !exists {
		log.Fatalf("Failed to find matching asset records for buy and hold symbol %s", symbol)
	}
	for _, record := range records.intradayRecords {
		if dateMin != nil && record.Timestamp.Before(*dateMin) {
			continue
		}
		if dateMax != nil && !record.Timestamp.Before(*dateMax) {
			continue
		}
		if record.Timestamp.Hour() != buyAndHoldTimeOfDay || record.Returns24H == nil {
			continue
		}
		side := SideLong
		if records.asset.ShortBias {
			side = SideShort
		}
		delta := record.Returns24H.Close2 - record.Returns24H.Close1
		returns := getAssetReturns(side, record.Timestamp, delta, false, &records.asset)
		cash += returns
		equityCurve.add(record.Timestamp, cash)
	}
	if equityCurve.empty() {
		log.Fatalf("Failed to retrieve buy and hold equity curve for symbol \"%s\"", symbol)
	}
	return equityCurve
}