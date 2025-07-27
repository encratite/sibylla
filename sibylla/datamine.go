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
	"gonum.org/v1/gonum/stat"
	"gopkg.in/yaml.v3"
)

const dataMiningScript = "datamine.js"
const hoursPerYear = 365.25 * 24
const tradingDaysPerYear = 252
const riskAdjustedSegments = 3

type DataMiningConfiguration struct {
	Assets []string `yaml:"assets"`
	FeaturesOnly []string `yaml:"featuresOnly"`
	EnableShort bool `yaml:"enableShort"`
	StrategyLimit int `yaml:"strategyLimit"`
	StrategyFilter *StrategyFilter `yaml:"strategyFilter"`
	Drawdown float64 `yaml:"drawdown"`
	DateMin *SerializableDate `yaml:"dateMin"`
	DateMax *SerializableDate `yaml:"dateMax"`
	TimeMin *SerializableDuration `yaml:"timeMin"`
	TimeMax *SerializableDuration `yaml:"timeMax"`
	Weekdays []SerializableWeekday `yaml:"weekdays"`
	TradesMin int `yaml:"tradesMin"`
	TradesRatio float64 `yaml:"tradesRatio"`
	Thresholds [][]float64 `yaml:"thresholds"`
}

type StrategyFilter struct {
	Trades int `yaml:"trades"`
	Limit float64 `yaml:"limit"`
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
	timeOfDay *time.Duration
	equityCurve []equityCurveSample
	returnsSamples []float64
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
	Weekdays []int `json:"weekdays"`
	Thresholds [][]float64 `json:"thresholds"`
	Results []AssetMiningResults `json:"results"`
}

type AssetMiningResults struct {
	Symbol string `json:"symbol"`
	Plot string `json:"plot"`
	Strategies []StrategyMiningResult `json:"strategies"`
}

type StrategyMiningResult struct {
	Side int `json:"side"`
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
}

type StrategyFeature struct {
	Symbol string `json:"symbol"`
	Name string `json:"name"`
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

func DataMine(yamlPath string) {
	loadConfiguration()
	loadCurrencies()
	miningConfig := loadDataMiningConfiguration(yamlPath)
	launchProfiler()
	model := executeDataMiningConfig(miningConfig)
	runtime.GC()
	debug.FreeOSMemory()
	runBrowser("Data Mining", dataMiningScript, model, true)
}

func executeDataMiningConfig(miningConfig DataMiningConfiguration) DataMiningModel {
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
	model := processResults(taskResults, assetRecords, miningConfig)
	return model
}

func getAssetPaths(miningConfig DataMiningConfiguration) []assetPath {
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
	for i, asset1 := range assetRecords {
		if asset1.asset.FeaturesOnly || slices.Contains(miningConfig.FeaturesOnly, asset1.asset.Symbol) {
			continue
		}
		for j, asset2 := range assetRecords {
			for k, feature1 := range accessors {
				for l, feature2 := range accessors {
					if i == j && k >= l {
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
	for symbol := range assetResults {
		slices.SortFunc(assetResults[symbol], func (a, b dataMiningResult) int {
			return compareFloat64(b.riskAdjustedMin, a.riskAdjustedMin)
		})
		results := assetResults[symbol]
		if len(results) > miningConfig.StrategyLimit {
			results = results[:miningConfig.StrategyLimit]
		}
		assetResults[symbol] = results
	}
	dailyRecords := map[string][]DailyRecord{}
	for _, records := range assetRecords {
		key := records.asset.Symbol
		dailyRecords[key] = records.dailyRecords
	}
	clearDirectory(configuration.TempPath)
	model := getDataMiningModel(assetResults, dailyRecords, miningConfig)
	delta := time.Since(start)
	fmt.Printf("Finished post-processing results in %.2f s\n", delta.Seconds())
	return model
}

func newDataMiningThreshold(asset assetRecords, feature featureAccessor, minMax []float64) featureThreshold {
	return featureThreshold{
		asset: asset,
		feature: feature,
		min: minMax[0],
		max: minMax[1],
	}
}

func executeDataMiningTask(task dataMiningTask, bar *pb.ProgressBar, miningConfig DataMiningConfiguration) []dataMiningResult {
	threshold1 := &task[0]
	threshold2 := &task[1]
	returnsAccessors := getReturnsAccessors()
	results := []dataMiningResult{}
	sides := []positionSide{SideLong}
	if miningConfig.EnableShort {
		sides = append(sides, SideShort)
	}
	for _, returns := range returnsAccessors {
		for _, side := range sides {
			for timeOfDay := miningConfig.TimeMin.Duration;
				timeOfDay <= miningConfig.TimeMax.Duration;
				timeOfDay += time.Duration(1) * time.Hour {
				result := dataMiningResult{
					task: task,
					returns: returns,
					side: side,
					timeOfDay: &timeOfDay,
					equityCurve: []equityCurveSample{},
					returnsSamples: []float64{},
					cumulativeReturn: 1.0,
					cumulativeMax: 1.0,
					drawdownMax: 0.0,
					enabled: true,
				}
				results = append(results, result)
			}
		}
	}
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
			returns := getAssetReturns(result.side, record1.Timestamp, returnsRecord.Ticks, asset)
			cash += returns
			sample := equityCurveSample{
				timestamp: record1.Timestamp,
				cash: cash,
			}
			*equityCurve = append(*equityCurve, sample)
			if !math.IsNaN(returnsRecord.Percent) {
				factor := 1.0 + returnsRecord.Percent
				percent := returnsRecord.Percent
				if result.side == SideShort {
					factor = 1.0 / factor
					percent = factor - 1.0
				}
				result.returnsSamples = append(result.returnsSamples, percent)
				result.cumulativeReturn *= factor
				result.cumulativeMax = max(result.cumulativeMax, result.cumulativeReturn)
				drawdown := 1.0 - result.cumulativeReturn / result.cumulativeMax
				result.drawdownMax = max(result.drawdownMax, drawdown)
			}
		}
		if !stillWorking {
			break
		}
		for i := range results {
			result := &results[i]
			if result.enabled {
				drawdownExceeded := result.drawdownMax > miningConfig.Drawdown
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
	for i := range results {
		result := &results[i]
		if len(result.equityCurve) < miningConfig.TradesMin {
			result.disable()
			continue
		}
		if !result.enabled {
			continue
		}
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
		result.tradesRatio = getTradesRatio(result.equityCurve, threshold1.asset.intradayRecords, miningConfig)
		result.returnsSamples = nil
		if result.tradesRatio < miningConfig.TradesRatio {
			result.disable()
		}
	}
	bar.Increment()
	return results
}

func getRiskAdjusted(returnsSamples []float64) float64 {
	mean := stat.Mean(returnsSamples, nil)
	stdDev := stat.StdDev(returnsSamples, nil)
	riskAdjusted := mean / stdDev
	return riskAdjusted
}

func getAssetReturns(side positionSide, timestamp time.Time, ticks int, asset *Asset) float64 {
	if side == SideLong {
		ticks -= asset.Spread
	} else {
		ticks += asset.Spread
	}
	rawGains := float64(ticks) * asset.TickValue
	gains := convertCurrency(timestamp, rawGains, asset.Currency)
	if side == SideShort {
		gains = - gains
	}
	fees := asset.BrokerFee + asset.ExchangeFee
	gains -= fees
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

func getDataMiningModel(
	assetResults map[string][]dataMiningResult,
	dailyRecords map[string][]DailyRecord,
	miningConfig DataMiningConfiguration,
) DataMiningModel {
	model := DataMiningModel{
		DateMin: getDateStringPointer(miningConfig.DateMin),
		DateMax: getDateStringPointer(miningConfig.DateMax),
		TimeMin: getTimeOfDayStringPointer(miningConfig.TimeMin),
		TimeMax: getTimeOfDayStringPointer(miningConfig.TimeMax),
		Weekdays: getWeekdayInts(miningConfig.Weekdays),
		Thresholds: miningConfig.Thresholds,
		Results: []AssetMiningResults{},
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
		for i, result := range results {
			miningResult := getStrategyMiningResult(symbol, i + 1, result)
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

func getWeekdayInts(weekdays []SerializableWeekday) []int {
	output := []int{}
	for _, w := range weekdays {
		output = append(output, int(w.Weekday))
	}
	return output
}

func getStrategyMiningResult(
	symbol string,
	index int,
	result dataMiningResult,
) StrategyMiningResult {
	equityCurve := result.equityCurve
	first := equityCurve[0]
	last := equityCurve[len(equityCurve) - 1]
	returns := last.cash - first.cash
	fileName := fmt.Sprintf("%s.strategy%02d.png", symbol, index)
	dailyRecordsPlotPath := filepath.Join(configuration.TempPath, fileName)
	plotEquityCurve(equityCurve, dailyRecordsPlotPath)
	output := StrategyMiningResult{
		Side: int(result.side),
		TimeOfDay: nil,
		Features: []StrategyFeature{},
		Exit: result.returns.name,
		Returns: returns,
		RiskAdjusted: result.riskAdjusted,
		RiskAdjustedMin: result.riskAdjustedMin,
		RiskAdjustedRecent: result.riskAdjustedRecent,
		MaxDrawdown: result.drawdownMax,
		TradesRatio: result.tradesRatio,
		Plot: getFileURL(dailyRecordsPlotPath),
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
}