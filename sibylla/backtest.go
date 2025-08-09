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
	seasonalityMode bool
	weekday *time.Weekday
	enableStopLoss bool
	stopLoss *float64
	stopLossHit bool
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

type riskAdjustedData struct {
	riskAdjustedIS []float64
	riskAdjustedRecentIS []float64
	riskAdjustedOOS []float64
	allReturnsSamples []float64
}

type monthlyEquityKey struct {
	year int
	month int
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
	buyAndHoldEquityCurve, _ := getBuyAndHold(buyAndHoldSymbol, &backtestConfig.DateMin.Time, &backtestConfig.DateMax.Time, assetRecords)
	buyAndHoldPerformance := getPerformanceByMonth(backtestConfig.DateMin.Time, backtestConfig.DateMax.Time, buyAndHoldEquityCurve)
	riskAdjustedData := getRiskAdjustedData(comparisons, buyAndHoldPerformance, backtestConfig)
	printStats(riskAdjustedData, assetRecords, backtestConfig)
}

func getRiskAdjustedData(comparisons []backtestComparison, buyAndHoldPerformance []float64, backtestConfig BacktestConfiguration) riskAdjustedData {
	riskAdjustedIS := []float64{}
	riskAdjustedRecentIS := []float64{}
	riskAdjustedOOS := []float64{}
	allReturnsSamples := []float64{}
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
		performance := getPerformanceByMonth(backtestConfig.DateMin.Time, backtestConfig.DateMax.Time, comparison.completeBacktest.equityCurve)
		performanceCorrelation := stat.Correlation(performance, buyAndHoldPerformance, nil)
		fmt.Printf("\tIS RAR:              %.3f\n", comparison.isBacktest.riskAdjusted)
		fmt.Printf("\tIS RecRAR:           %.3f\n", comparison.isBacktest.riskAdjustedRecent)
		fmt.Printf("\tOOS RAR:             %.3f\n", comparison.oosBacktest.riskAdjusted)
		fmt.Printf("\tMarket correlation:  %.3f\n\n", performanceCorrelation)
		riskAdjustedIS = append(riskAdjustedIS, comparison.isBacktest.riskAdjusted)
		riskAdjustedRecentIS = append(riskAdjustedRecentIS, comparison.isBacktest.riskAdjustedRecent)
		riskAdjustedOOS = append(riskAdjustedOOS, comparison.oosBacktest.riskAdjusted)
		allReturnsSamples = append(allReturnsSamples, comparison.oosBacktest.returnsSamples...)
	}
	return riskAdjustedData{
		riskAdjustedIS: riskAdjustedIS,
		riskAdjustedRecentIS: riskAdjustedRecentIS,
		riskAdjustedOOS: riskAdjustedOOS,
		allReturnsSamples: allReturnsSamples,
	}
}

func printStats(
	riskAdjustedData riskAdjustedData,
	assetRecords []assetRecords,
	backtestConfig BacktestConfiguration,
) {
	strategyCount := len(backtestConfig.Strategies)
	fmt.Printf("IS period: %s to %s\n", getDateString(backtestConfig.DateMin.Time), getDateString(backtestConfig.DateSplit.Time))
	fmt.Printf("OOS period: %s to %s\n", getDateString(backtestConfig.DateSplit.Time), getDateString(backtestConfig.DateMax.Time))
	fmt.Printf("Number of strategies: %d\n\n", strategyCount)
	riskAdjustedCorrelation := stat.Correlation(riskAdjustedData.riskAdjustedIS, riskAdjustedData.riskAdjustedOOS, nil)
	riskAdjustedRecentCorrelation := stat.Correlation(riskAdjustedData.riskAdjustedRecentIS, riskAdjustedData.riskAdjustedOOS, nil)
	fmt.Printf("PCC(IS RAR, OOS RAR):    %.3f\n", riskAdjustedCorrelation)
	fmt.Printf("PCC(IS RecRAR, OOS RAR): %.3f\n\n", riskAdjustedRecentCorrelation)
	_, buyAndHoldReturnsIS := getBuyAndHold(buyAndHoldSymbol, &backtestConfig.DateMin.Time, &backtestConfig.DateSplit.Time, assetRecords)
	_, buyAndHoldReturnsOOS := getBuyAndHold(buyAndHoldSymbol, &backtestConfig.DateSplit.Time, &backtestConfig.DateMax.Time, assetRecords)
	buyAndHoldRiskAdjustedIS := getRiskAdjusted(buyAndHoldReturnsIS)
	buyAndHoldRiskAdjustedOOS := getRiskAdjusted(buyAndHoldReturnsOOS)
	allRiskAdjustedOOS := getRiskAdjusted(riskAdjustedData.allReturnsSamples)
	fmt.Printf("Buy and Hold IS RAR:  %.3f\n", buyAndHoldRiskAdjustedIS)
	fmt.Printf("Buy and Hold OOS RAR: %.3f\n\n", buyAndHoldRiskAdjustedOOS)
	meanRiskAdjustedIS := stat.Mean(riskAdjustedData.riskAdjustedIS, nil)
	meanRiskAdjustedRecentIS := stat.Mean(riskAdjustedData.riskAdjustedRecentIS, nil)
	meanRiskAdjustedOOS := stat.Mean(riskAdjustedData.riskAdjustedOOS, nil)
	fmt.Printf("Mean(IS RAR):         %.3f\n", meanRiskAdjustedIS)
	fmt.Printf("Mean(IS RecRAR):      %.3f\n", meanRiskAdjustedRecentIS)
	fmt.Printf("Mean(OOS RAR):        %.3f\n\n", meanRiskAdjustedOOS)
	fmt.Printf("Portfolio OOS RAR:    %.3f\n\n", allRiskAdjustedOOS)
	printClassifications(buyAndHoldRiskAdjustedOOS, riskAdjustedData.riskAdjustedOOS, strategyCount)
}

func printClassifications(
	buyAndHoldRiskAdjustedOOS float64,
	riskAdjustedOOS []float64,
	strategyCount int,
) {
	outperform := 0
	underperform := 0
	loss := 0
	for _, riskAdjusted := range riskAdjustedOOS {
		if riskAdjusted > buyAndHoldRiskAdjustedOOS {
			outperform++
		} else if riskAdjusted > 0 {
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
	backtest := newBacktest(strategy.Symbol, strategy.Side.PositionSide, &strategy.Time.Duration, conditions, returns)
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
			onConditionMatch(record, &tradedAsset.asset, backtestConfig.Leverage, false, &backtest)
		}
	}
	backtest.postProcess(true, true, backtestConfig.DateMin.Time, backtestConfig.DateMax.Time, intradayRecords)
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
	delta := returnsRecord.Close2 - returnsRecord.Close1
	if backtest.enableStopLoss {
		processStopLoss(&delta,	returnsRecord, backtest)
	}
	returns := getAssetReturns(backtest.side, record.Timestamp, delta, true, asset)
	notionalValue := float64(returnsRecord.Close1) * asset.TickValue
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

func processStopLoss(
	delta *int,
	returnsRecord *ReturnsRecord,
	backtest *backtestData,
) {
	if backtest.side == SideLong {
		drawdown := 1.0 - float64(returnsRecord.Low) / float64(returnsRecord.Close1)
		if drawdown > *backtest.stopLoss {
			stopLossLevel := int((1.0 - *backtest.stopLoss) * float64(returnsRecord.Close1))
			*delta = stopLossLevel - returnsRecord.Close1
			backtest.stopLossHit = true
		}
	} else {
		drawdown := float64(returnsRecord.High) / float64(returnsRecord.Close1) - 1.0
		if drawdown > *backtest.stopLoss {
			stopLossLevel := int((1.0 + *backtest.stopLoss) * float64(returnsRecord.Close1))
			*delta = stopLossLevel - returnsRecord.Close1
			backtest.stopLossHit = true
		}
	}
}

func (backtest *backtestData) postProcess(
	setRiskAdjusted bool,
	preserveReturnsSamples bool,
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
	}
	if !preserveReturnsSamples {
		backtest.returnsSamples = nil
	}
}

func newBacktest(
	symbol string,
	side PositionSide,
	timeOfDay *time.Duration,
	conditions []strategyCondition,
	returns returnsAccessor,
) backtestData {
	return backtestData{
		symbol: symbol,
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
		enableStopLoss: false,
		stopLoss: nil,
		stopLossHit: false,
	}
}

func getBuyAndHold(symbol string, dateMin *time.Time, dateMax *time.Time, assets []assetRecords) ([]equityCurveSample, []float64) {
	equityCurve := []equityCurveSample{}
	returnsSamples := []float64{}
	cash := 0.0
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
		sample := equityCurveSample{
			timestamp: record.Timestamp,
			cash: cash,
		}
		equityCurve = append(equityCurve, sample)
		notionalValue := float64(record.Returns24H.Close1) * records.asset.TickValue
		percent := returns / notionalValue
		returnsSamples = append(returnsSamples, percent)

	}
	if len(equityCurve) == 0 {
		log.Fatalf("Failed to retrieve buy and hold equity curve for symbol \"%s\"", symbol)
	}
	return equityCurve, returnsSamples
}

func newMonthlyEquityKey(timestamp time.Time) monthlyEquityKey {
	return monthlyEquityKey{
		year: timestamp.Year(),
		month: int(timestamp.Month()),
	}
}

func getPerformanceByMonth(dateMin time.Time, dateMax time.Time, equityCurve []equityCurveSample) []float64 {
	const initialCash = 50000.0
	cashMap := map[monthlyEquityKey]equityCurveSample{}
	for _, sample := range equityCurve {
		key := newMonthlyEquityKey(sample.timestamp)
		mapSample, exists := cashMap[key]
		if exists && sample.timestamp.Before(mapSample.timestamp) {
			continue
		}
		cashMap[key] = sample
	}
	cash := initialCash
	output := []float64{}
	for date := dateMin; date.Before(dateMax); date = date.AddDate(0, 1, 0) {
		key := newMonthlyEquityKey(date)
		mapSample, exists := cashMap[key]
		returns := 0.0
		if exists {
			newCash := initialCash + mapSample.cash
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