package sibylla

import (
	"fmt"
	
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Asset struct {
	Symbol string `yaml:"symbol"`
	BarchartSymbol string `yaml:"barchartSymbol"`
	Name string `yaml:"name"`

	// Contract filtering fields
	LegacyCutoff *GlobexCode `yaml:"legacyCutoff"`
	FirstFilterContract *GlobexCode `yaml:"firstFilterContract"`
	LastFilterContract *GlobexCode `yaml:"lastFilterContract"`
	IncludeMonths []string `yaml:"includeMonths"`
	ExcludeMonths []string `yaml:"excludeMonths"`
	ExcludeRecords []*ConfigTime `yaml:"excludeRecords"`
	CutoffDate *ConfigDate `yaml:"cutoffDate"`
	FRecords *int `yaml:"fRecords"`
	FeaturesOnly bool `yaml:"featuresOnly"`

	// Asset definition
	Currency string `yaml:"currency"`
	TickSize SerializableDecimal `yaml:"tickSize"`
	TickValue SerializableDecimal `yaml:"tickValue"`
	Margin SerializableDecimal `yaml:"margin"`
	BrokerFee SerializableDecimal `yaml:"brokerFee"`
	ExchangeFee SerializableDecimal `yaml:"exchangeFee"`
	Spread int `yaml:"spread"`
}

type ConfigDate struct {
	time.Time
}

type ConfigTime struct {
	time.Time
}

const enableFilterDebugOutput = false

func (a *Asset) getBarchartSymbol() string {
	symbol := a.Symbol
	if a.BarchartSymbol != "" {
		symbol = a.BarchartSymbol
	}
	return symbol
}

func (a *Asset) includeRecord(date time.Time, symbol GlobexCode) bool {
	if a.CutoffDate != nil && date.Before(a.CutoffDate.Time) {
		if enableFilterDebugOutput {
			fmt.Printf("Excluded %s due to CutOffDate %s\n", symbol, getDateString((*a.CutoffDate).Time))
		}
		return false
	}
	if a.LegacyCutoff != nil && symbol.Less(*a.LegacyCutoff) {
		if enableFilterDebugOutput {
			fmt.Printf("Excluded %s due to LegacyCutoff %s\n", symbol, a.LegacyCutoff)
		}
		return false
	}
	if a.FirstFilterContract != nil && a.LastFilterContract != nil {
		if symbol.Less(*a.FirstFilterContract) || !symbol.Less(*a.LastFilterContract) {
			return true
		}
	} else if a.FirstFilterContract != nil && symbol.Less(*a.FirstFilterContract) {
		return true
	} else if a.LastFilterContract != nil && !symbol.Less(*a.LastFilterContract) {
		return true
	}
	if a.IncludeMonths != nil {
		include := containsString(symbol.Month, a.IncludeMonths)
		if enableFilterDebugOutput && !include {
			fmt.Printf("Excluded %s due to IncludeMonths %s\n", symbol, strings.Join(a.IncludeMonths, ", "))
		}
		return include
	} else if a.ExcludeMonths != nil {
		include := !containsString(symbol.Month, a.ExcludeMonths)
		if enableFilterDebugOutput && !include {
			fmt.Printf("Excluded %s due to ExcludeMonths %s\n", symbol, strings.Join(a.ExcludeMonths, ", "))
		}
		return include
	}
	return true
}

func (c *ConfigDate) UnmarshalYAML(value *yaml.Node) error {
	date, err := getDateErr(value.Value)
	if err != nil {
		return err
	}
	*c = ConfigDate{date}
	return nil
}

func (c *ConfigTime) UnmarshalYAML(value *yaml.Node) error {
	timestamp, err := getTimeErr(value.Value)
	if err != nil {
		return err
	}
	*c = ConfigTime{timestamp}
	return nil
}

func containsString(target string, strings []string) bool {
	for _, x := range strings {
		if x == target {
			return true
		}
	}
	return false
}