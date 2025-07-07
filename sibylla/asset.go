package sibylla

type Asset struct {
	Symbol string `yaml:"symbol"`
	BarchartSymbol string `yaml:"barchartSymbol"`
	Name string `yaml:"name"`
	LegacyCutoff string `yaml:"legacyCutoff"`
	Currency string `yaml:"currency"`
	TickSize SerializableDecimal `yaml:"tickSize"`
	TickValue SerializableDecimal `yaml:"tickValue"`
	Margin SerializableDecimal `yaml:"margin"`
	BrokerFee SerializableDecimal `yaml:"brokerFee"`
	ExchangeFee SerializableDecimal `yaml:"exchangeFee"`
	Spread int `yaml:"spread"`
}

func (definition *Asset) getBarchartSymbol() string {
	symbol := definition.Symbol
	if definition.BarchartSymbol != "" {
		symbol = definition.BarchartSymbol
	}
	return symbol
}
