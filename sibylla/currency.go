package sibylla

import (
	"fmt"
	"log"
	"path/filepath"
	"time"
)

const currencyUSD = "USD"
const currencyEUR = "EUR"
const currencyJPY = "JPY"

type currencyMap struct {
	symbol string
	records map[time.Time]float64
}

var currencies *[]currencyMap

func loadCurrencies() {
	symbols := []string{
		currencyEUR,
		currencyJPY,
	}
	currencyMaps := []currencyMap{}
	for _, symbol := range symbols {
		currencyMap := loadCurrency(symbol)
		currencyMaps = append(currencyMaps, currencyMap)
	}
	currencies = &currencyMaps
}

func loadCurrency(symbol string) currencyMap {
	filename := fmt.Sprintf("^%s%s.H1.csv", symbol, currencyUSD)
	path := filepath.Join(configuration.BarchartPath, filename)
	columns := []string{"time", "close"}
	records := map[time.Time]float64{}
	callback := func(values []string) {
		timestamp := getTime(values[0])
		close := parseFloat(values[1])
		records[timestamp] = close
	}
	readCsv(path, columns, callback)
	currencyMap := currencyMap{
		symbol: symbol,
		records: records,
	}
	return currencyMap
}

// Warning: this function currently ignores timezones and spreads
func convertCurrency(timestamp time.Time, amount float64, symbol string) float64 {
	if symbol == currencyUSD {
		return amount
	}
	for _, currencyMap := range *currencies {
		if currencyMap.symbol == symbol {
			for i := 0; i < 50; i++ {
				close, exists := currencyMap.records[timestamp]
				if exists {
					converted := close * amount
					return converted
				}
				timestamp = timestamp.Add(- time.Hour)
			}
			log.Fatalf("Failed to find a matching record for timestamp %s for currency %s", getTimeString(timestamp), symbol)
		}
	}
	log.Fatalf("Failed to find currency %s", symbol)
	return 0.0
}