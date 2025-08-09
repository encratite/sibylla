package sibylla

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"time"

	"github.com/cheggaaa/pb"
	"gonum.org/v1/gonum/stat"
	"gopkg.in/yaml.v3"
)

const dataMiningScript = "datamine.js"
const hoursPerYear = 365.25 * 24
const tradingDaysPerYear = 252
const riskAdjustedSegments = 3
const daysPerWeek = 5
const weekdayOptimizationBuffer = 35
const recentWeekdayPlotSamples = 100

type DataMiningConfiguration struct {
	Assets []string `yaml:"assets"`
	FeaturesOnly []string `yaml:"featuresOnly"`
	EnableLong bool `yaml:"enableLong"`
	EnableShort bool `yaml:"enableShort"`
	StrategyLimit int `yaml:"strategyLimit"`
	StrategyFilter *StrategyFilter `yaml:"strategyFilter"`
	Drawdown float64 `yaml:"drawdown"`
	DateMin SerializableDate `yaml:"dateMin"`
	DateMax SerializableDate `yaml:"dateMax"`
	TimeMin SerializableDuration `yaml:"timeMin"`
	TimeMax SerializableDuration `yaml:"timeMax"`
	OptimizeWeekdays bool `yaml:"optimizeWeekdays"`
	TradesMin int `yaml:"tradesMin"`
	TradesRatio float64 `yaml:"tradesRatio"`
	Conditions ConditionConfiguration `yaml:"conditions"`
	Leverage *float64 `yaml:"leverage"`
	SingleFeature bool `yaml:"singleFeature"`
	SeasonalityMode bool `yaml:"seasonalityMode"`
	CorrelationSplits []SerializableDate `yaml:"correlationSplits"`
	StrategyRatio *float64 `yaml:"strategyRatio"`
}

type StrategyFilter struct {
	Trades int `yaml:"trades"`
	Limit float64 `yaml:"limit"`
}

type ConditionConfiguration struct {
	Range float64 `yaml:"range"`
	Increment float64 `yaml:"increment"`
}

type DataMiningModel struct {
	DateMin string `json:"dateMin"`
	DateMax string `json:"dateMax"`
	TimeMin string `json:"timeMin"`
	TimeMax string `json:"timeMax"`
	OptimizeWeekdays bool `json:"optimizeWeeks"`
	Conditions *DataMiningConditions `json:"conditions"`
	Results []AssetMiningResults `json:"results"`
	Features *FeatureAnalysis `json:"features"`
	SeasonalityMode bool `json:"seasonalityMode"`
}

type DataMiningConditions struct {
	Range float64 `json:"range"`
	Increment float64 `json:"increment"`
}

type AssetMiningResults struct {
	Symbol string `json:"symbol"`
	Plot string `json:"plot"`
	Strategies []StrategyMiningResult `json:"strategies"`
}

type StrategyMiningResult struct {
	Side int `json:"side"`
	OptimizeWeekdays bool `json:"optimizeWeekdays"`
	Weekday *int `json:"weekday"`
	TimeOfDay *string `json:"timeOfDay"`
	Features []StrategyFeature `json:"features"`
	Exit string `json:"exit"`
	Returns float64 `json:"returns"`
	RiskAdjusted float64 `json:"riskAdjusted"`
	RiskAdjustedMin float64 `json:"riskAdjustedMin"`
	RiskAdjustedRecent float64 `json:"riskAdjustedRecent"`
	MaxDrawdown float64 `json:"maxDrawdown"`
	TradesRatio float64 `json:"tradesRatio"`
	Plot string `json:"plot"`
	WeekdayPlot string `json:"weekdayPlot"`
	RecentPlot string `json:"recentPlot"`
}

type StrategyFeature struct {
	Symbol string `json:"symbol"`
	Name string `json:"name"`
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

type FeatureFrequency struct {
	Name string `json:"name"`
	Frequencies []float64 `json:"frequencies"`
}

type FeatureAnalysis struct {
	Features []FeatureFrequency `json:"features"`
	Combinations [][]float64 `json:"combinations"`
}

type dataMiningTask struct {
	conditions []strategyCondition
	seasonality *seasonalityTask
}

type seasonalityTask struct {
	asset assetRecords
	weekday time.Weekday
}

func DataMine(yamlPath string) {
	loadConfiguration()
	loadCurrencies()
	miningConfig := loadDataMiningConfiguration(yamlPath)
	launchProfiler()
	taskResults, assetRecords := executeDataMiningConfig(miningConfig)
	model := processResults(taskResults, assetRecords, miningConfig)
	runtime.GC()
	debug.FreeOSMemory()
	runBrowser("Data Mining", dataMiningScript, model, true)
}

func executeDataMiningConfig(miningConfig DataMiningConfiguration) ([][]backtestData, []assetRecords)  {
	assetRecords := getAssetRecords(
		miningConfig.Assets,
		miningConfig.DateMin,
		miningConfig.DateMax,
		&miningConfig.TimeMin,
		&miningConfig.TimeMax,
	)
	start := time.Now()
	tasks := getDataMiningTasks(assetRecords, miningConfig)
	fmt.Println("Data mining strategies")
	bar := pb.StartNew(len(tasks))
	bar.Start()
	taskResults := parallelMap(tasks, func (task dataMiningTask) []backtestData {
		return executeDataMiningTask(task, bar, miningConfig)
	})
	bar.Finish()
	delta := time.Since(start)
	fmt.Printf("Finished data mining in %.2f s\n", delta.Seconds())
	return taskResults, assetRecords
}

func getDataMiningTasks(assetRecords []assetRecords, miningConfig DataMiningConfiguration) []dataMiningTask {
	if miningConfig.SeasonalityMode {
		return getSeasonalityMiningTasks(assetRecords)
	} else {
		return getFeatureMiningTasks(assetRecords, miningConfig)
	}
}

func getSeasonalityMiningTasks(assetRecords []assetRecords) []dataMiningTask {
	tasks := []dataMiningTask{}
	for _, asset := range assetRecords {
		for weekday := time.Monday; weekday <= time.Friday; weekday++ {
			seasonality := seasonalityTask{
				asset: asset,
				weekday: weekday,
			}
			task := dataMiningTask{
				seasonality: &seasonality,
			}
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func getFeatureMiningTasks(assetRecords []assetRecords, miningConfig DataMiningConfiguration) []dataMiningTask {
	accessors := getFeatureAccessors()
	tasks := []dataMiningTask{}
	conditionRange := miningConfig.Conditions.Range
	increment := miningConfig.Conditions.Increment
	const epsilonLimit = 1.0 + 1e-3
	singleFeature := miningConfig.SingleFeature
	for i, asset1 := range assetRecords {
		if asset1.asset.FeaturesOnly || slices.Contains(miningConfig.FeaturesOnly, asset1.asset.Symbol) {
			continue
		}
		for j, asset2 := range assetRecords {
			for k, feature1 := range accessors {
				for l, feature2 := range accessors {
					if !singleFeature && i == j && k >= l {
						continue
					}
					if singleFeature && (i != j || k != l) {
						continue
					}
					for min1 := 0.0; min1 + conditionRange <= epsilonLimit; min1 += increment {
						for min2 := 0.0; min2 + conditionRange <= epsilonLimit; min2 += increment {
							if singleFeature && min1 != min2 {
								continue
							}
							max1 := min1 + conditionRange
							max2 := min2 + conditionRange
							parameter1 := newDataMiningParameter(asset1, feature1, min1, max1)
							parameter2 := newDataMiningParameter(asset2, feature2, min2, max2)
							parameters := []strategyCondition{
								parameter1,
								parameter2,
							}
							task := dataMiningTask{
								conditions: parameters,
							}
							tasks = append(tasks, task)
						}
					}
				}
			}
		}
	}
	return tasks
}

func processResults(
	taskResults [][]backtestData,
	assetRecords []assetRecords,
	miningConfig DataMiningConfiguration,
) DataMiningModel {
	start := time.Now()
	assetResults := map[string][]backtestData{}
	for _, results := range taskResults {
		for _, result := range results {
			if result.enabled {
				key := result.symbol
				assetResults[key] = append(assetResults[key], result)
			}
		}
	}
	if len(assetResults) == 0 {
		log.Fatal("No results")
	}
	analyzeWeekdayOptimizations(assetResults)
	analysis := analyzeFeatureFrequency(assetResults, miningConfig)
	for symbol := range assetResults {
		slices.SortFunc(assetResults[symbol], func (a, b backtestData) int {
			return compareFloat64(b.riskAdjustedMin, a.riskAdjustedMin)
		})
		results := assetResults[symbol]
		if len(results) > miningConfig.StrategyLimit {
			results = results[:miningConfig.StrategyLimit]
		}
		slices.SortFunc(results, func (a, b backtestData) int {
			return compareFloat64(b.riskAdjustedRecent, a.riskAdjustedRecent)
		})
		assetResults[symbol] = results
	}
	dailyRecords := map[string][]DailyRecord{}
	for _, records := range assetRecords {
		key := records.asset.Symbol
		dailyRecords[key] = records.dailyRecords
	}
	clearDirectory(configuration.TempPath)
	model := getDataMiningModel(assetResults, dailyRecords, assetRecords, analysis, miningConfig)
	delta := time.Since(start)
	fmt.Printf("Finished post-processing results in %.2f s\n", delta.Seconds())
	return model
}

func newDataMiningParameter(asset assetRecords, feature featureAccessor, min float64, max float64) strategyCondition {
	return strategyCondition{
		asset: asset,
		feature: feature,
		min: min,
		max: max,
	}
}

func executeDataMiningTask(task dataMiningTask, bar *pb.ProgressBar, miningConfig DataMiningConfiguration) []backtestData {
	var backtests []backtestData
	if miningConfig.SeasonalityMode {
		backtests = executeSeasonalityMiningTask(task, miningConfig)
	} else {
		backtests = executeFeatureMiningTask(task, miningConfig)
	}
	bar.Increment()
	return backtests
}

func executeSeasonalityMiningTask(task dataMiningTask, miningConfig DataMiningConfiguration) []backtestData {
	records := task.seasonality.asset.intradayRecords
	backtests := initializeMiningBacktests(task, miningConfig)
	for i := range records {
		record := &records[i]
		if !record.hasReturns() {
			continue
		}
		if record.Timestamp.Weekday() != task.seasonality.weekday {
			continue
		}
		for j := range backtests {
			backtest := &backtests[j]
			onConditionMatch(record, &task.seasonality.asset.asset, miningConfig.Leverage, false, backtest)
		}
	}
	postProcessBacktests(records, backtests, miningConfig)
	return backtests
}

func executeFeatureMiningTask(task dataMiningTask, miningConfig DataMiningConfiguration) []backtestData {
	condition1 := &task.conditions[0]
	condition2 := &task.conditions[1]
	backtests := initializeMiningBacktests(task, miningConfig)
	for i := range condition1.asset.intradayRecords {
		record1 := &condition1.asset.intradayRecords[i]
		if !record1.hasReturns() || !condition1.match(record1) {
			continue
		}
		record2, exists := condition2.asset.recordsMap[record1.Timestamp]
		if !exists || !condition2.match(record2) {
			continue
		}
		asset := &condition1.asset.asset
		stillWorking := onDataMiningConditionMatch(record1, asset, backtests, miningConfig)
		if !stillWorking {
			break
		}
		for j := range backtests {
			backtest := &backtests[j]
			if backtest.enabled {
				drawdownExceeded := !miningConfig.isCorrelation() && backtest.drawdownMax > miningConfig.Drawdown
				var enoughSamples, badPerformance bool
				if miningConfig.StrategyFilter != nil {
					enoughSamples = len(backtest.equityCurve) >= miningConfig.StrategyFilter.Trades
					badPerformance = backtest.cumulativeReturn < miningConfig.StrategyFilter.Limit
				} else {
					enoughSamples = false
					badPerformance = false
				}
				if drawdownExceeded || (enoughSamples && badPerformance) {
					backtest.disable()
				}
			}
		}
	}
	postProcessBacktests(condition1.asset.intradayRecords, backtests, miningConfig)
	return backtests
}

func onDataMiningConditionMatch(
	record1 *FeatureRecord,
	asset *Asset,
	backtests []backtestData,
	miningConfig DataMiningConfiguration,
) bool {
	stillWorking := false
	for j := range backtests {
		backtest := &backtests[j]
		if !backtest.enabled {
			continue
		}
		stillWorking = true
		addTimestamp := miningConfig.isCorrelation()
		onConditionMatch(record1, asset, miningConfig.Leverage, addTimestamp, backtest)
	}
	return stillWorking
}

func initializeMiningBacktests(task dataMiningTask, miningConfig DataMiningConfiguration) []backtestData {
	backtests := []backtestData{}
	sides := []PositionSide{}
	if miningConfig.EnableLong {
		sides = append(sides, SideLong)
	}
	if miningConfig.EnableShort {
		sides = append(sides, SideShort)
	}
	optimizeWeekdaysModes := []bool{false}
	if miningConfig.OptimizeWeekdays {
		optimizeWeekdaysModes = append(optimizeWeekdaysModes, true)
	}
	returnsAccessors := getReturnsAccessors()
	var symbol string
	if task.seasonality != nil {
		symbol = task.seasonality.asset.asset.Symbol
	} else {
		symbol = task.conditions[0].asset.asset.Symbol
	}
	for _, returns := range returnsAccessors {
		for _, side := range sides {
			for _, optimizeWeekdays := range optimizeWeekdaysModes {
				for timeOfDay := miningConfig.TimeMin.Duration;
					timeOfDay <= miningConfig.TimeMax.Duration;
					timeOfDay += time.Duration(1) * time.Hour {
					backtest := newBacktest(symbol, side, &timeOfDay, task.conditions, returns)
					backtest.optimizeWeekdays = optimizeWeekdays
					if task.seasonality != nil {
						backtest.seasonalityMode = true
						backtest.weekday = &task.seasonality.weekday
					}
					for i := range backtest.optimizationReturns {
						backtest.optimizationReturns[i].SetBaseCap(weekdayOptimizationBuffer + 2)
					}
					backtests = append(backtests, backtest)
				}
			}
		}
	}
	return backtests
}

func optimizeWeekdays(percent float64, weekdayIndex int, backtest *backtestData) {
	weekdayReturns := &backtest.optimizationReturns[weekdayIndex]
	weekdayReturns.PushBack(percent)
	for weekdayReturns.Len() > weekdayOptimizationBuffer {
		weekdayReturns.PopFront()
	}
	buffersFilled := true
	for _, x := range backtest.optimizationReturns {
		if x.Len() < weekdayOptimizationBuffer {
			buffersFilled = false
			break
		}
	}
	if buffersFilled {
		weekdayPerformance := [daysPerWeek]float64{}
		for k := range backtest.optimizationReturns {
			performance := 1.0
			currentWeekday := backtest.optimizationReturns[k]
			for l := 0; l < currentWeekday.Len(); l++ {
				performance *= 1.0 + currentWeekday.At(l)
			}
			weekdayPerformance[k] = performance
		}
		worstIndex := 0
		worstPerformance := weekdayPerformance[0]
		for k := 1; k < len(weekdayPerformance); k++ {
			performance := weekdayPerformance[k]
			if performance < worstPerformance {
				worstIndex = k
				worstPerformance = performance
			}
		}
		bannedDay := time.Weekday(worstIndex + 1)
		backtest.bannedDay = &bannedDay
	}
}

func postProcessBacktests(intradayRecords []FeatureRecord, backtests []backtestData, miningConfig DataMiningConfiguration) {
	firstYear := miningConfig.DateMin.Time.Year()
	lastDate := miningConfig.DateMax.Time
	lastYear := lastDate.Year()
	if lastDate.Month() == 1 {
		lastYear--
	}
	for i := range backtests {
		backtest := &backtests[i]
		if len(backtest.equityCurve) < miningConfig.TradesMin {
			backtest.disable()
			continue
		}
		if !backtest.enabled {
			continue
		}
		years := map[int]struct{}{}
		for _, sample := range backtest.equityCurve {
			year := sample.timestamp.Year()
			years[year] = struct{}{}
		}
		disable := false
		for year := lastYear; year >= firstYear; year-- {
			_, exists := years[year]
			if !exists {
				disable = true
				break
			}
		}
		if disable {
			backtest.disable()
			continue
		}
		setRiskAdjusted := !miningConfig.isCorrelation()
		preserveReturnsSamples := !setRiskAdjusted
		backtest.postProcess(setRiskAdjusted, preserveReturnsSamples, miningConfig.DateMin.Time, miningConfig.DateMax.Time, intradayRecords)
		if backtest.tradesRatio < miningConfig.TradesRatio {
			backtest.disable()
		}
	}
}

func getRiskAdjusted(returnsSamples []float64) float64 {
	mean, stdDev := stat.MeanStdDev(returnsSamples, nil)
	riskAdjusted := mean / stdDev
	return riskAdjusted
}

func getAssetReturns(side PositionSide, timestamp time.Time, ticks int, enableFees bool, asset *Asset) float64 {
	if enableFees {
		if side == SideLong {
			ticks -= asset.Spread
		} else {
			ticks += asset.Spread
		}
	}
	rawGains := float64(ticks) * asset.TickValue
	gains := convertCurrency(timestamp, rawGains, asset.Currency)
	if side == SideShort {
		gains = - gains
	}
	if enableFees {
		fees := asset.BrokerFee + asset.ExchangeFee
		gains -= fees
	}
	return gains
}

func (c *strategyCondition) match(record *FeatureRecord) bool {
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
	configuration.Assets = append(configuration.Assets, configuration.FeaturesOnly...)
	return *configuration
}

func (c *DataMiningConfiguration) sanityCheck() {
	if c.StrategyFilter.Limit == 0 || c.StrategyFilter.Limit == 0.0 {
		log.Fatal("Invalid strategy filter configuration")
	}
	if c.Conditions.Increment == 0.0 || c.Conditions.Range == 0.0 {
		log.Fatal("Invalid condition configuration")
	}
	if !c.DateMin.Before(c.DateMax.Time) {
		format := "Invalid dateMin/dateMax values in data mining configuration: %s vs. %s"
		log.Fatalf(format, getDateString(c.DateMin.Time), getDateString(c.DateMax.Time))
	}
	if c.TimeMin.Duration > c.TimeMax.Duration {
		format := "Invalid timeMin/timeMax values in data mining configuration: %s vs. %s"
		log.Fatalf(format, c.TimeMin, c.TimeMax)
	}
	if c.TimeMin.Duration > c.TimeMax.Duration {
		log.Fatalf("Data mining without timeMin/timeMax constraints is no longer supported")
	}
}

func (c *DataMiningConfiguration) isCorrelation() bool {
	return c.CorrelationSplits != nil
}

func getDataMiningModel(
	assetResults map[string][]backtestData,
	dailyRecords map[string][]DailyRecord,
	assetRecords []assetRecords,
	analysis *featureAnalysis,
	miningConfig DataMiningConfiguration,
) DataMiningModel {
	features := getFeatureModel(analysis)
	model := DataMiningModel{
		DateMin: getDateString(miningConfig.DateMin.Time),
		DateMax: getDateString(miningConfig.DateMax.Time),
		TimeMin: getTimeOfDayString(miningConfig.TimeMin.Duration),
		TimeMax: getTimeOfDayString(miningConfig.TimeMax.Duration),
		OptimizeWeekdays: miningConfig.OptimizeWeekdays,
		Results: []AssetMiningResults{},
		Features: features,
		SeasonalityMode: miningConfig.SeasonalityMode,
	}
	if !miningConfig.SeasonalityMode {
		conditions := DataMiningConditions{
			Range: miningConfig.Conditions.Range,
			Increment: miningConfig.Conditions.Increment,
		}
		model.Conditions = &conditions
	}
	symbols := []string{}
	for symbol := range assetResults {
		symbols = append(symbols, symbol)
	}
	model.Results = parallelMap(symbols, func (symbol string) AssetMiningResults {
		results, exists := assetResults[symbol]
		if !exists {
			log.Fatalf("Unable to find matching results for symbol %s", symbol)
		}
		plotRecords, exists := dailyRecords[symbol]
		if !exists {
			log.Fatalf("Unable to find matching daily records for symbol %s", symbol)
		}
		fileName := fmt.Sprintf("%s.daily.png", symbol)
		dailyRecordsPlotPath := filepath.Join(configuration.TempPath, fileName)
		plotDailyRecords(plotRecords, dailyRecordsPlotPath)
		assetMiningResults := AssetMiningResults{
			Symbol: symbol,
			Plot: getFileURL(dailyRecordsPlotPath),
			Strategies: []StrategyMiningResult{},
		}
		buyAndHold, _ := getBuyAndHold(symbol, &miningConfig.DateMin.Time, &miningConfig.DateMax.Time, assetRecords)
		for i, result := range results {
			miningResult := getStrategyMiningResult(symbol, i + 1, result, buyAndHold)
			assetMiningResults.Strategies = append(assetMiningResults.Strategies, miningResult)
		}
		return assetMiningResults
	})
	slices.SortFunc(model.Results, func (a, b AssetMiningResults) int {
		index1 := slices.Index(miningConfig.Assets, a.Symbol)
		index2 := slices.Index(miningConfig.Assets, b.Symbol)
		return index1 - index2
	})
	return model
}

func getStrategyMiningResult(
	symbol string,
	index int,
	result backtestData,
	buyAndHold []equityCurveSample,
) StrategyMiningResult {
	equityCurve := result.equityCurve
	first := equityCurve[0]
	last := equityCurve[len(equityCurve) - 1]
	returns := last.cash - first.cash
	plotURL, weekdayPlotURL, recentPlotURL := createStrategyPlots(symbol, index, result, buyAndHold)
	output := StrategyMiningResult{
		Side: int(result.side),
		OptimizeWeekdays: result.optimizeWeekdays,
		Weekday: nil,
		TimeOfDay: nil,
		Features: []StrategyFeature{},
		Exit: result.returns.name,
		Returns: returns,
		RiskAdjusted: result.riskAdjusted,
		RiskAdjustedMin: result.riskAdjustedMin,
		RiskAdjustedRecent: result.riskAdjustedRecent,
		MaxDrawdown: result.drawdownMax,
		TradesRatio: result.tradesRatio,
		Plot: plotURL,
		WeekdayPlot: weekdayPlotURL,
		RecentPlot: recentPlotURL,
	}
	if result.timeOfDay != nil {
		timeOfDayString := getTimeOfDayString(*result.timeOfDay)
		output.TimeOfDay = &timeOfDayString
	}
	if result.weekday != nil {
		weekday := int(*result.weekday)
		output.Weekday = &weekday
	}
	for _, parameter := range result.conditions {
		feature := StrategyFeature{
			Symbol: parameter.asset.asset.Symbol,
			Name: parameter.feature.name,
			Min: parameter.min,
			Max: parameter.max,
		}
		output.Features = append(output.Features, feature)
	}
	return output
}

func createStrategyPlots(
	symbol string,
	index int,
	result backtestData,
	buyAndHold []equityCurveSample,
) (string, string, string) {
	plotFileName := fmt.Sprintf("%s.strategy%02d.png", symbol, index)
	plotPath := filepath.Join(configuration.TempPath, plotFileName)
	plotEquityCurve(result.equityCurve, buyAndHold, plotPath)
	weekdayPlotFilename := fmt.Sprintf("%s.strategy%02d.weekday.png", symbol, index)
	weekdayPlotPath := filepath.Join(configuration.TempPath, weekdayPlotFilename)
	plotWeekdayReturns("Mean Return by Weekday (All)", result.weekdayReturns, weekdayPlotPath)
	recentPlotFilename := fmt.Sprintf("%s.strategy%02d.weekday.recent.png", symbol, index)
	recentPlotPath := filepath.Join(configuration.TempPath, recentPlotFilename)
	recentWeekDayReturns := [daysPerWeek][]float64{}
	for i := range result.weekdayReturns {
		truncated := result.weekdayReturns[i]
		if len(truncated) > recentWeekdayPlotSamples {
			truncated = truncated[len(truncated) - recentWeekdayPlotSamples:]
		}
		recentWeekDayReturns[i] = truncated
	}
	plotWeekdayReturns("Mean Return by Weekday (Recent)", recentWeekDayReturns, recentPlotPath)
	plotURL := getFileURL(plotPath)
	weekdayPlotURL := getFileURL(weekdayPlotPath)
	recentPlotURL := getFileURL(recentPlotPath)
	return plotURL, weekdayPlotURL, recentPlotURL
}

func getTradesRatio(
	dateMin time.Time,
	dateMax time.Time,
	equityCurve []equityCurveSample,
	intradayRecords []FeatureRecord,
) float64 {
	recordsFirst := intradayRecords[0].Timestamp
	recordsLast := intradayRecords[len(intradayRecords) - 1].Timestamp
	start := dateMin
	if start.Before(recordsFirst) {
		start = recordsFirst
	}
	end := dateMax
	if recordsLast.After(end) {
		end = recordsLast
	}
	duration := end.Sub(start)
	daysTradedMap := map[time.Time]struct{}{}
	for _, record := range equityCurve {
		date := getDateFromTime(record.timestamp)
		daysTradedMap[date] = struct{}{}
	}
	daysTraded := len(daysTradedMap)
	years := duration.Hours() / hoursPerYear
	tradesRatio := float64(daysTraded) / (tradingDaysPerYear * years)
	return tradesRatio
}

func (backtest *backtestData) disable() {
	backtest.enabled = false
	backtest.equityCurve = nil
	backtest.returnsSamples = nil
	for i := range backtest.weekdayReturns {
		backtest.weekdayReturns[i] = nil
	}
	for i := range backtest.optimizationReturns {
		backtest.optimizationReturns[i].Clear()
	}
}