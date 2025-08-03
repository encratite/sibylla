package sibylla

import (
	"fmt"
	"log"
	"math"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/gammazero/deque"
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
const buyAndHoldTimeOfDay = 12

type DataMiningConfiguration struct {
	Assets []string `yaml:"assets"`
	FeaturesOnly []string `yaml:"featuresOnly"`
	EnableLong bool `yaml:"enableLong"`
	EnableShort bool `yaml:"enableShort"`
	StrategyLimit int `yaml:"strategyLimit"`
	StrategyFilter *StrategyFilter `yaml:"strategyFilter"`
	Drawdown float64 `yaml:"drawdown"`
	DateMin *SerializableDate `yaml:"dateMin"`
	DateMax *SerializableDate `yaml:"dateMax"`
	TimeMin *SerializableDuration `yaml:"timeMin"`
	TimeMax *SerializableDuration `yaml:"timeMax"`
	OptimizeWeekdays bool `yaml:"optimizeWeekdays"`
	TradesMin int `yaml:"tradesMin"`
	TradesRatio float64 `yaml:"tradesRatio"`
	Thresholds TresholdConfiguration `yaml:"thresholds"`
	Leverage *float64 `yaml:"leverage"`
	SingleFeature bool `yaml:"singleFeature"`
	CorrelationSplits []SerializableDate `yaml:"correlationSplits"`
	StrategyRatio *float64 `yaml:"strategyRatio"`
}

type StrategyFilter struct {
	Trades int `yaml:"trades"`
	Limit float64 `yaml:"limit"`
}

type TresholdConfiguration struct {
	Range float64 `yaml:"range"`
	Increment float64 `yaml:"increment"`
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
	dailyRecords []DailyRecord
	intradayRecords []FeatureRecord
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

type DataMiningModel struct {
	DateMin *string `json:"dateMin"`
	DateMax *string `json:"dateMax"`
	TimeMin *string `json:"timeMin"`
	TimeMax *string `json:"timeMax"`
	OptimizeWeekdays bool `json:"optimizeWeeks"`
	Thresholds DataMiningThresholds `json:"thresholds"`
	Results []AssetMiningResults `json:"results"`
	Features FeatureAnalysis `json:"features"`
}

type DataMiningThresholds struct {
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

func executeDataMiningConfig(miningConfig DataMiningConfiguration) ([][]dataMiningResult, []assetRecords)  {
	assetPaths := getAssetPaths(miningConfig)
	start := time.Now()
	assetRecords := parallelMap(assetPaths, func (path assetPath) assetRecords {
		return getAssetRecords(path, miningConfig)
	})
	delta := time.Since(start)
	fmt.Printf("Loaded archives in %.2f s\n", delta.Seconds())
	start = time.Now()
	tasks := getDataMiningTasks(assetRecords, miningConfig)
	fmt.Println("Data mining strategies")
	bar := pb.StartNew(len(tasks))
	bar.Start()
	taskResults := parallelMap(tasks, func (task dataMiningTask) []dataMiningResult {
		return executeDataMiningTask(task, bar, miningConfig)
	})
	bar.Finish()
	delta = time.Since(start)
	fmt.Printf("Finished data mining in %.2f s\n", delta.Seconds())
	return taskResults, assetRecords
}

func getAssetPaths(miningConfig DataMiningConfiguration) []assetPath {
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
			if len(miningConfig.Assets) > 0 && !contains(miningConfig.Assets, asset.Symbol) {
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

func getAssetRecords(assetPath assetPath, miningConfig DataMiningConfiguration) assetRecords {
	archive := readArchive(assetPath.path)
	dailyRecords := []DailyRecord{}
	intradayRecords := []FeatureRecord{}
	recordsMap := map[time.Time]*FeatureRecord{}
	for _, record := range archive.DailyRecords {
		isValid, breakLoop := miningConfig.isValidDate(record.Date)
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
		isValid, breakLoop := miningConfig.isValidDate(record.Timestamp)
		if !isValid {
			if breakLoop {
				break
			} else {
				continue
			}
		}
		isValid = miningConfig.isValidTime(record.Timestamp)
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

func getDataMiningTasks(assetRecords []assetRecords, miningConfig DataMiningConfiguration) []dataMiningTask {
	accessors := getFeatureAccessors()
	tasks := []dataMiningTask{}
	thresholdRange := miningConfig.Thresholds.Range
	increment := miningConfig.Thresholds.Increment
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
					for min1 := 0.0; min1 + thresholdRange <= epsilonLimit; min1 += increment {
						for min2 := 0.0; min2 + thresholdRange <= epsilonLimit; min2 += increment {
							if singleFeature && min1 != min2 {
								continue
							}
							max1 := min1 + thresholdRange
							max2 := min2 + thresholdRange
							threshold1 := newDataMiningThreshold(asset1, feature1, min1, max1)
							threshold2 := newDataMiningThreshold(asset2, feature2, min2, max2)
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

func processResults(
	taskResults [][]dataMiningResult,
	assetRecords []assetRecords,
	miningConfig DataMiningConfiguration,
) DataMiningModel {
	start := time.Now()
	assetResults := map[string][]dataMiningResult{}
	for _, results := range taskResults {
		for _, result := range results {
			if result.enabled {
				key := result.task[0].asset.asset.Symbol
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
		slices.SortFunc(assetResults[symbol], func (a, b dataMiningResult) int {
			return compareFloat64(b.riskAdjustedMin, a.riskAdjustedMin)
			// return compareFloat64(b.equityCurve[len(b.equityCurve) - 1].cash, a.equityCurve[len(a.equityCurve) - 1].cash)
		})
		results := assetResults[symbol]
		if len(results) > miningConfig.StrategyLimit {
			results = results[:miningConfig.StrategyLimit]
		}
		slices.SortFunc(results, func (a, b dataMiningResult) int {
			return compareFloat64(b.riskAdjustedRecent, a.riskAdjustedRecent)
			// return compareFloat64(b.equityCurve[len(b.equityCurve) - 1].cash, a.equityCurve[len(a.equityCurve) - 1].cash)
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

func newDataMiningThreshold(asset assetRecords, feature featureAccessor, min float64, max float64) featureThreshold {
	return featureThreshold{
		asset: asset,
		feature: feature,
		min: min,
		max: max,
	}
}

func executeDataMiningTask(task dataMiningTask, bar *pb.ProgressBar, miningConfig DataMiningConfiguration) []dataMiningResult {
	threshold1 := &task[0]
	threshold2 := &task[1]
	results := initializeMiningResults(task, miningConfig)
	for i := range threshold1.asset.intradayRecords {
		record1 := &threshold1.asset.intradayRecords[i]
		if !record1.hasReturns() || !threshold1.match(record1) {
			continue
		}
		record2, exists := threshold2.asset.recordsMap[record1.Timestamp]
		if !exists || !threshold2.match(record2) {
			continue
		}
		asset := &threshold1.asset.asset
		stillWorking := onThresholdMatch(record1, asset, results, miningConfig)
		if !stillWorking {
			break
		}
		for i := range results {
			result := &results[i]
			if result.enabled {
				drawdownExceeded := !miningConfig.isCorrelation() && result.drawdownMax > miningConfig.Drawdown
				var enoughSamples, badPerformance bool
				if miningConfig.StrategyFilter != nil {
					enoughSamples = len(result.equityCurve) >= miningConfig.StrategyFilter.Trades
					badPerformance = result.cumulativeReturn < miningConfig.StrategyFilter.Limit
				} else {
					enoughSamples = false
					badPerformance = false
				}
				if drawdownExceeded || (enoughSamples && badPerformance) {
					result.disable()
				}
			}
		}
	}
	postProcessMiningResults(threshold1.asset.intradayRecords, results, miningConfig)
	bar.Increment()
	return results
}

func onThresholdMatch(
	record1 *FeatureRecord,
	asset *Asset,
	results []dataMiningResult,
	miningConfig DataMiningConfiguration,
) bool {
	stillWorking := false
	for j := range results {
		result := &results[j]
		if !result.enabled {
			continue
		}
		stillWorking = true
		if result.timeOfDay != nil {
			timeOfDay := getTimeOfDay(record1.Timestamp)
			if timeOfDay != *result.timeOfDay {
				continue
			}
		}
		returnsRecord := result.returns.get(record1)
		if returnsRecord == nil {
			continue
		}
		if math.IsNaN(returnsRecord.Percent) {
			continue
		}
		cash := 0.0
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
		factor := 1.0 + returnsRecord.Percent
		percent := returnsRecord.Percent
		if result.side == SideShort {
			factor = 1.0 / factor
			percent = factor - 1.0
		}
		weekdayIndex := int(record1.Timestamp.Weekday()) - 1
		bannedDay := result.bannedDay
		if result.optimizeWeekdays {
			optimizeWeekdays(percent, weekdayIndex, result)
		}
		if bannedDay != nil && record1.Timestamp.Weekday() == *bannedDay {
			continue
		}
		returns := getAssetReturns(result.side, record1.Timestamp, returnsRecord.Ticks, true, asset)
		if miningConfig.Leverage != nil {
			returns *= *miningConfig.Leverage
		}
		cash += returns
		sample := equityCurveSample{
			timestamp: record1.Timestamp,
			cash: cash,
		}
		*equityCurve = append(*equityCurve, sample)
		result.returnsSamples = append(result.returnsSamples, percent)
		if miningConfig.isCorrelation() {
			result.returnsTimestamps = append(result.returnsTimestamps, record1.Timestamp)
		}
		result.weekdayReturns[weekdayIndex] = append(result.weekdayReturns[weekdayIndex], percent)
		result.cumulativeReturn *= factor
		result.cumulativeMax = max(result.cumulativeMax, result.cumulativeReturn)
		drawdown := 1.0 - result.cumulativeReturn / result.cumulativeMax
		result.drawdownMax = max(result.drawdownMax, drawdown)
	}
	return stillWorking
}

func initializeMiningResults(task dataMiningTask, miningConfig DataMiningConfiguration) []dataMiningResult {
	results := []dataMiningResult{}
	sides := []positionSide{}
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
	for _, returns := range returnsAccessors {
		for _, side := range sides {
			for _, optimizeWeekdays := range optimizeWeekdaysModes {
				for timeOfDay := miningConfig.TimeMin.Duration;
					timeOfDay <= miningConfig.TimeMax.Duration;
					timeOfDay += time.Duration(1) * time.Hour {
					if timeOfDay.Hours() != 16 {
						continue
					}
					result := dataMiningResult{
						task: task,
						returns: returns,
						side: side,
						optimizeWeekdays: optimizeWeekdays,
						timeOfDay: &timeOfDay,
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
					for i := range result.optimizationReturns {
						result.optimizationReturns[i].SetBaseCap(weekdayOptimizationBuffer + 2)
					}
					results = append(results, result)
				}
			}
		}
	}
	return results
}

func optimizeWeekdays(percent float64, weekdayIndex int, result *dataMiningResult) {
	weekdayReturns := &result.optimizationReturns[weekdayIndex]
	weekdayReturns.PushBack(percent)
	for weekdayReturns.Len() > weekdayOptimizationBuffer {
		weekdayReturns.PopFront()
	}
	buffersFilled := true
	for _, x := range result.optimizationReturns {
		if x.Len() < weekdayOptimizationBuffer {
			buffersFilled = false
			break
		}
	}
	if buffersFilled {
		weekdayPerformance := [daysPerWeek]float64{}
		for k := range result.optimizationReturns {
			performance := 1.0
			currentWeekday := result.optimizationReturns[k]
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
		result.bannedDay = &bannedDay
	}
}

func postProcessMiningResults(intradayRecords []FeatureRecord, results []dataMiningResult, miningConfig DataMiningConfiguration) {
	firstYear := miningConfig.DateMin.Time.Year()
	lastDate := miningConfig.DateMax.Time
	lastYear := lastDate.Year()
	if lastDate.Month() == 1 {
		lastYear--
	}
	for i := range results {
		result := &results[i]
		if len(result.equityCurve) < miningConfig.TradesMin {
			result.disable()
			continue
		}
		if !result.enabled {
			continue
		}
		years := map[int]struct{}{}
		for _, sample := range result.equityCurve {
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
			result.disable()
			continue
		}
		result.tradesRatio = getTradesRatio(result.equityCurve, intradayRecords, miningConfig)
		if !miningConfig.isCorrelation() {
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
		if result.tradesRatio < miningConfig.TradesRatio {
			result.disable()
		}
	}
}

func getRiskAdjusted(returnsSamples []float64) float64 {
	mean, stdDev := stat.MeanStdDev(returnsSamples, nil)
	riskAdjusted := mean / stdDev
	return riskAdjusted
}

func getAssetReturns(side positionSide, timestamp time.Time, ticks int, enableFees bool, asset *Asset) float64 {
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
	configuration.Assets = append(configuration.Assets, configuration.FeaturesOnly...)
	return *configuration
}

func (c *DataMiningConfiguration) sanityCheck() {
	if c.StrategyFilter.Limit == 0 || c.StrategyFilter.Limit == 0.0 {
		log.Fatal("Invalid strategy filter configuration")
	}
	if c.Thresholds.Increment == 0.0 || c.Thresholds.Range == 0.0 {
		log.Fatal("Invalid threshold configuration")
	}
	if c.DateMin != nil && c.DateMax != nil && !c.DateMin.Before(c.DateMax.Time) {
		format := "Invalid dateMin/dateMax values in data mining configuration: %s vs. %s"
		log.Fatalf(format, getDateString(c.DateMin.Time), getDateString(c.DateMax.Time))
	}
	if c.TimeMin != nil && c.TimeMax != nil && c.TimeMin.Duration > c.TimeMax.Duration {
		format := "Invalid timeMin/timeMax values in data mining configuration: %s vs. %s"
		log.Fatalf(format, *c.TimeMin, *c.TimeMax)
	}
	if c.TimeMin == nil || c.TimeMax == nil {
		log.Fatalf("Data mining without timeMin/timeMax constraints is no longer supported")
	}
}

func (c *DataMiningConfiguration) isValidDate(timestamp time.Time) (bool, bool) {
	if c.DateMin != nil && timestamp.Before(c.DateMin.Time) {
		return false, false
	}
	if c.DateMax != nil && !timestamp.Before(c.DateMax.Time) {
		return false, true
	}
	return true, false
}

func (c *DataMiningConfiguration) isValidTime(timestamp time.Time) bool {
	date := getDateFromTime(timestamp)
	timeOfDay := timestamp.Sub(date)
	if c.TimeMin != nil && timeOfDay < c.TimeMin.Duration {
		return false
	}
	if c.TimeMax != nil && timeOfDay > c.TimeMax.Duration {
		return false
	}
	return true
}

func (c *DataMiningConfiguration) isCorrelation() bool {
	return c.CorrelationSplits != nil
}

func getDataMiningModel(
	assetResults map[string][]dataMiningResult,
	dailyRecords map[string][]DailyRecord,
	assetRecords []assetRecords,
	analysis featureAnalysis,
	miningConfig DataMiningConfiguration,
) DataMiningModel {
	features := getFeatureModel(analysis)
	model := DataMiningModel{
		DateMin: getDateStringPointer(miningConfig.DateMin),
		DateMax: getDateStringPointer(miningConfig.DateMax),
		TimeMin: getTimeOfDayStringPointer(miningConfig.TimeMin),
		TimeMax: getTimeOfDayStringPointer(miningConfig.TimeMax),
		OptimizeWeekdays: miningConfig.OptimizeWeekdays,
		Thresholds: DataMiningThresholds{
			Range: miningConfig.Thresholds.Range,
			Increment: miningConfig.Thresholds.Increment,
		},
		Results: []AssetMiningResults{},
		Features: features,
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
		buyAndHold := getBuyAndHold(symbol, assetRecords)
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

func getDateStringPointer(date *SerializableDate) *string {
	if date != nil {
		output := getDateString(date.Time)
		return &output
	} else {
		return nil
	}
}

func getTimeOfDayStringPointer(t *SerializableDuration) *string {
	if t != nil {
		output := fmt.Sprintf("%02d:%02d", int(t.Hours()), int(t.Minutes()) % 60)
		return &output
	} else {
		return nil
	}
}

func getStrategyMiningResult(
	symbol string,
	index int,
	result dataMiningResult,
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
	for _, threshold := range result.task {
		feature := StrategyFeature{
			Symbol: threshold.asset.asset.Symbol,
			Name: threshold.feature.name,
			Min: threshold.min,
			Max: threshold.max,
		}
		output.Features = append(output.Features, feature)
	}
	return output
}

func createStrategyPlots(
	symbol string,
	index int,
	result dataMiningResult,
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
	equityCurve []equityCurveSample,
	intradayRecords []FeatureRecord,
	miningConfig DataMiningConfiguration,
) float64 {
	equityFirst := equityCurve[0].timestamp
	equityLast := equityCurve[len(equityCurve) - 1].timestamp
	recordsFirst := intradayRecords[0].Timestamp
	recordsLast := intradayRecords[len(intradayRecords) - 1].Timestamp
	var start, end time.Time
	if miningConfig.DateMin != nil {
		start = (*miningConfig.DateMin).Time
	} else {
		start = equityFirst
	}
	if start.Before(recordsFirst) {
		start = recordsFirst
	}
	if miningConfig.DateMax != nil {
		end = (*miningConfig.DateMax).Time
	} else {
		end = equityLast
	}
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

func (d *dataMiningResult) disable() {
	d.enabled = false
	d.equityCurve = nil
	d.returnsSamples = nil
	for i := range d.weekdayReturns {
		d.weekdayReturns[i] = nil
	}
	for i := range d.optimizationReturns {
		d.optimizationReturns[i].Clear()
	}
}

func getBuyAndHold(symbol string, allRecords []assetRecords) []equityCurveSample {
	equityCurve := []equityCurveSample{}
	cash := 0.0
	index := slices.IndexFunc(allRecords, func (x assetRecords) bool {
		return x.asset.Symbol == symbol
	})
	if index == -1 {
		log.Fatalf("Failed to find matching asset records for buy and hold symbol %s", symbol)
	}
	records := allRecords[index]
	for _, record := range records.intradayRecords {
		if record.Timestamp.Hour() != buyAndHoldTimeOfDay || record.Returns24H == nil {
			continue
		}
		side := SideLong
		if records.asset.ShortBias {
			side = SideShort
		}
		returns := getAssetReturns(side, record.Timestamp, record.Returns24H.Ticks, false, &records.asset)
		cash += returns
		sample := equityCurveSample{
			timestamp: record.Timestamp,
			cash: cash,
		}
		equityCurve = append(equityCurve, sample)
	}
	return equityCurve
}