package sibylla

type Asset struct {
	Symbol string `yaml:"symbol"`
	BarchartSymbol string `yaml:"barchartSymbol"`
	Name string `yaml:"name"`
	LegacyCutoff string `yaml:"legacyCutoff"`
	Currency string `yaml:"currency"`
	TickSize serializableDecimal `yaml:"tickSize"`
	TickValue serializableDecimal `yaml:"tickValue"`
	Margin serializableDecimal `yaml:"margin"`
	BrokerFee serializableDecimal `yaml:"brokerFee"`
	ExchangeFee serializableDecimal `yaml:"exchangeFee"`
	Spread int `yaml:"spread"`
}

func (definition *Asset) getBarchartSymbol() string {
	symbol := definition.Symbol
	if definition.BarchartSymbol != "" {
		symbol = definition.BarchartSymbol
	}
	return symbol
}
