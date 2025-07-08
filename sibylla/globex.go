package sibylla

import (
	"fmt"
	"log"
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

func (g GlobexCode) Less(other GlobexCode) bool {
	if g.Root != other.Root {
		log.Fatalf("Tried to compare Globex codes with different roots (%s vs. %s)", g.Root, other.Root)
	}
	if g.Year < other.Year {
		return true
	} else if g.Year == other.Year && g.Month < other.Month {
		return true
	} else {
		return false
	}
}

func (g GlobexCode) String() string {
	return fmt.Sprintf("%s%s%02d", g.Root, g.Month, g.Year % 100)
}

func (g *GlobexCode) UnmarshalYAML(value *yaml.Node) error {
	symbol, err := parseGlobex(value.Value)
	if err != nil {
		return err
	}
	*g = symbol
	return nil
}