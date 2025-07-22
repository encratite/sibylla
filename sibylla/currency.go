package sibylla

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/shopspring/decimal"
)

const currencyUSD = "USD"
const currencyEUR = "EUR"
const currencyJPY = "JPY"

type currencyMap struct {
	symbol string
	records map[time.Time]decimal.Decimal
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
	records := map[time.Time]decimal.Decimal{}
	callback := func(values []string) {
		timestamp := getTime(values[0])
		close := getDecimal(values[1], path)
		records[timestamp] = close.Decimal
	}
	readCsv(path, columns, callback)
	currencyMap := currencyMap{
		symbol: symbol,
		records: records,
	}
	return currencyMap
}

// Warning: this function currently ignores timezones and spreads
func convertCurrency(timestamp time.Time, amount decimal.Decimal, symbol string) decimal.Decimal {
	if symbol == currencyUSD {
		return amount
	}
	for _, currencyMap := range *currencies {
		if currencyMap.symbol == symbol {
			for i := 0; i < 5; i++ {
				close, exists := currencyMap.records[timestamp]
				if exists {
					converted := close.Mul(amount)
					return converted
				}
				timestamp = timestamp.Add(- time.Hour)
			}
			log.Fatalf("Failed to find a matching record for timestamp %s for currency %s", getTimeString(timestamp), symbol)
		}
	}
	log.Fatalf("Failed to find currency %s", symbol)
	return decimal.Decimal{}
}