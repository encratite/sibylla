package sibylla

import (
	"fmt"
	"regexp"
	"strconv"

	"gopkg.in/yaml.v3"
)

var globexPattern = regexp.MustCompile("^([A-Z0-9]{2,})([FGHJKMNQUVXZ])([0-9]{2})$")

type GlobexCode struct {
	Root string
	Month string
	Year int
}

func parseGlobex(symbol string) (GlobexCode, error) {
	matches := globexPattern.FindStringSubmatch(symbol)
	if matches == nil {
		return GlobexCode{}, fmt.Errorf("unable to parse Globex code: %s", symbol)
	}
	root := matches[1]
	month := matches[2]
	yearString := matches[3]
	year, err := strconv.Atoi(yearString)
	if err != nil {
		return GlobexCode{}, fmt.Errorf("failed to convert year string \"%s\" to integer: %v", yearString, err)
	}
	if year < 70 {
		year += 2000
	} else {
		year += 1900
	}
	output := GlobexCode{
		Root: root,
		Month: month,
		Year: year,
	}
	return output, nil
}

func (g *GlobexCode) UnmarshalYAML(value *yaml.Node) error {
	symbol, err := parseGlobex(value.Value)
	if err != nil {
		return err
	}
	*g = symbol
	return nil
}