package sibylla

import (
	"image/color"
	"log"
	"math"
	"slices"
	"time"

	"golang.org/x/image/font/opentype"
	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/font"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

type YearlyTicks struct{}

type MoneyTicks struct{}

func plotDailyRecords(records []DailyRecord, path string) {
	plotterData := make(plotter.XYs, len(records))
	for i, record := range records {
		plotterData[i].X = timeToFloat(record.Date)
		plotterData[i].Y = record.Close
	}
	plotLine("Close", plotterData, false, path)
}

func plotEquityCurve(equityCurve []equityCurveSample, path string) {
	plotterData := make(plotter.XYs, len(equityCurve))
	for i, sample := range equityCurve {
		plotterData[i].X = timeToFloat(sample.timestamp)
		plotterData[i].Y = sample.cash
	}
	plotLine("Money", plotterData, true, path)
}

func plotLine(yLabel string, plotterData plotter.XYs, money bool, path string) {
	ttfData := readFile(configuration.FontPath)
	openTypeFont, err := opentype.Parse(ttfData)
	if err != nil {
		log.Fatal("OpenType failed to parse TTF file:", err)
	}
	defaultFont := font.Font{
		Typeface: font.Typeface(configuration.FontName),
	}
	fontFace := []font.Face{
		{
			Font: defaultFont,
			Face: openTypeFont,
		},
	}
	font.DefaultCache.Add(fontFace)
	plot.DefaultFont = defaultFont
	p := plot.New()
	p.X.Label.Text = "Date"
	p.Y.Label.Text = yLabel
	p.X.Padding = -1
	p.Y.Padding = -1
	grid := plotter.NewGrid()
	dashes := []vg.Length{vg.Points(2), vg.Points(2)}
	grid.Horizontal.Dashes = dashes
	grid.Vertical.Dashes = dashes
	p.Add(grid)
	p.X.Tick.Marker = YearlyTicks{}
	if money {
		p.Y.Tick.Marker = MoneyTicks{}
	}
	line, err := plotter.NewLine(plotterData)
	if err != nil {
		log.Fatal("Failed to create line plot:", err)
	}
	line.LineStyle.Color = color.RGBA{R: 255, A: 255}
	p.Add(line)
	err = p.Save(12 * vg.Inch, 8 * vg.Inch, path)
	if err != nil {
		log.Fatalf("Failed to save plot (%s): %v", path, err)
	}
}

func plotFeatureHistogram(stdDev float64, values []float64, path string) {
	valuesMin := slices.Min(values)
	valuesMax := slices.Max(values)
	var bins int
	quantiles := epsilonCompare(valuesMin, 0.0) && epsilonCompare(valuesMax, 1.0)
	if quantiles {
		bins = 30
	} else {
		bins = 50
	}
	plotterValues := make(plotter.Values, len(values))
	copy(plotterValues, values)
	p := plot.New()
	h, err := plotter.NewHist(plotterValues, bins)
	if err != nil {
		log.Fatal("Failed to create histogram plot:", err)
	}
	h.Normalize(1)
	p.Add(h)
	if quantiles {
		p.X.Min = 0
		p.X.Max = 1
	} else {
		xLimit := 6 * stdDev
		p.X.Min = - xLimit
		p.X.Max = xLimit
	}
	err = p.Save(8 * vg.Inch, 4 * vg.Inch, path)
	if err != nil {
		log.Fatalf("Failed to save plot (%s): %v", path, err)
	}
}

func plotWeekdayReturns(weekdayReturns map[time.Weekday][]float64, path string) {
	labels := []string{
		"Mon",
		"Tue",
		"Wed",
		"Thu",
		"Fri",
	}
	values := make(plotter.Values, len(labels))
	for i := range labels {
		samples, exists := weekdayReturns[time.Weekday(i + 1)]
		var mean float64
		if exists {
			mean = stat.Mean(samples, nil)
		} else {
			mean = 0.0
		}
		values[i] = mean
	}
	p := plot.New()
	p.NominalX(labels...)
	bars, err := plotter.NewBarChart(values, vg.Points(25))
	if err != nil {
		log.Fatal("Failed to create bar chart:", err)
	}
	bars.LineStyle.Width = 0
	bars.Color = color.RGBA{R: 255, A: 255}
	p.Add(bars)
	err = p.Save(4.5 * vg.Inch, 3 * vg.Inch, path)
	if err != nil {
		log.Fatalf("Failed to save plot (%s): %v", path, err)
	}
}

func epsilonCompare(a float64, b float64) bool {
	return math.Abs(a - b) <= 1e-3
}

func (YearlyTicks) Ticks(min, max float64) []plot.Tick {
	timeMin := time.Unix(int64(min), 0).UTC()
	timeMax := time.Unix(int64(max), 0).UTC()
	year := timeMin.Year()
	ticks := []plot.Tick{}
	for y := year + 1; y <= timeMax.Year(); y += 2 {
		tickTime := time.Date(y, time.January, 1, 0, 0, 0, 0, time.UTC)
		x := timeToFloat(tickTime)
		label := tickTime.Format("2006")
		ticks = append(ticks, plot.Tick{Value: x, Label: label})
	}
	return ticks
}

func (MoneyTicks) Ticks(min, max float64) []plot.Tick {
	ticks := plot.DefaultTicks{}.Ticks(min, max)
	for i := range ticks {
		if ticks[i].Label != "" {
			amount := int64(ticks[i].Value)
			ticks[i].Label = formatMoney(amount)
		}
	}
	return ticks
}

func timeToFloat(t time.Time) float64 {
	return float64(t.Unix())
}