package sibylla

import (
	"image/color"
	"log"
	"time"

	"golang.org/x/image/font/opentype"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/font"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

type YearlyTicks struct{}

func plotDailyRecords(title string, records []DailyRecord, path string) {
	plotterData := make(plotter.XYs, len(records))
	for i, dataPoint := range records {
		plotterData[i].X = timeToFloat(dataPoint.Date)
		float, _ := dataPoint.Close.Float64()
		plotterData[i].Y = float
	}
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
	p.Title.Text = title
	p.X.Label.Text = "Date"
	p.Y.Label.Text = "Close"
	p.X.Padding = -1
	p.Y.Padding = -1
	grid := plotter.NewGrid()
	dashes := []vg.Length{vg.Points(2), vg.Points(2)}
	grid.Horizontal.Dashes = dashes
	grid.Vertical.Dashes = dashes
	p.Add(grid)
	p.X.Tick.Marker = YearlyTicks{}
	line, err := plotter.NewLine(plotterData)
	if err != nil {
		log.Fatal("Failed to create line plot:", err)
	}
	line.LineStyle.Color = color.RGBA{R: 255, A: 255}
	p.Add(line)
	err = p.Save(12 * vg.Inch, 8 * vg.Inch, path)
	if err != nil {
		log.Fatal("Failed to save plot:", err)
	}	
}

func plotFeatureHistogram(title string, stdDev float64, values []float64, path string) {
	plotterValues := make(plotter.Values, len(values))
	copy(plotterValues, values)
	p := plot.New()
	p.Title.Text = title
	h, err := plotter.NewHist(plotterValues, 50)
	if err != nil {
		panic(err)
	}
	h.Normalize(1)
	p.Add(h)
	xLimit := 6 * stdDev
	p.X.Min = - xLimit
	p.X.Max = xLimit
	err = p.Save(8 * vg.Inch, 4 * vg.Inch, path)
	if err != nil {
		panic(err)
	}
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

func timeToFloat(t time.Time) float64 {
	return float64(t.Unix())
}