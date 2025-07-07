package sibylla

import (
	"time"

	"gopkg.in/yaml.v3"
)

type Asset struct {
	Symbol string `yaml:"symbol"`
	BarchartSymbol string `yaml:"barchartSymbol"`
	Name string `yaml:"name"`

	// Contract filtering fields
	FRecordsLimit *int `yaml:"fRecordsLimit"`
	EnableFYRecords bool `yaml:"enableFYRecords"`
	LegacyCutoff *GlobexCode `yaml:"legacyCutoff"`
	FirstFilterContract *GlobexCode `yaml:"firstFilterContract"`
	LastFilterContract *GlobexCode `yaml:"lastFilterContract"`
	IncludeMonths []string `yaml:"includeMonths"`
	ExcludeMonths []string `yaml:"excludeMonths"`
	CutoffDate *ConfigDate `yaml:"cutoffDate"`
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

func (definition *Asset) getBarchartSymbol() string {
	symbol := definition.Symbol
	if definition.BarchartSymbol != "" {
		symbol = definition.BarchartSymbol
	}
	return symbol
}

func (c *ConfigDate) UnmarshalYAML(value *yaml.Node) error {
	date, err := getDate(value.Value)
	if err != nil {
		return err
	}
	*c = ConfigDate{date}
	return nil
}
