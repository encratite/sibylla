package sibylla

import (
	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

type serializableDecimal struct {
	decimal.Decimal
}

func (d *serializableDecimal) UnmarshalYAML(value *yaml.Node) error {
	decimalValue, err := decimal.NewFromString(value.Value)
	if err != nil {
		return err
	}
	d.Decimal = decimalValue
	return nil
}

func (d serializableDecimal) MarshalYAML() (interface{}, error) {
	return d.Decimal.String(), nil
}
