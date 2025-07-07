package sibylla

import (
	"log"
	"regexp"
	"strconv"
)

var globexPattern = regexp.MustCompile("^([A-Z0-9]{2,})([FGHJKMNQUVXZ])([0-9]{2})$")

type globexCode struct {
	root string
	month string
	year int
}

func globexFromString(symbol string) globexCode {
	matches := globexPattern.FindStringSubmatch(symbol)
	if matches == nil {
		log.Fatalf("Unable to parse Globex code: %s", symbol)
	}
	root := matches[1]
	month := matches[2]
	yearString := matches[3]
	year, err := strconv.Atoi(yearString)
	if err != nil {
		log.Fatalf("Failed to convert year string \"%s\" to integer: %v", yearString, err)
	}
	if year < 70 {
		year += 2000
	} else {
		year += 1900
	}
	output := globexCode{
		root: root,
		month: month,
		year: year,
	}
	return output
}
