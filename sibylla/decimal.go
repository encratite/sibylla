package sibylla

import (
	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

type SerializableDecimal struct {
	decimal.Decimal
}

func (d *SerializableDecimal) UnmarshalYAML(value *yaml.Node) error {
	decimalValue, err := decimal.NewFromString(value.Value)
	if err != nil {
		return err
	}
	d.Decimal = decimalValue
	return nil
}