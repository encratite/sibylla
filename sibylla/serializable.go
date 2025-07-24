package sibylla

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type SerializableDate struct {
	time.Time
}

type SerializableDuration struct {
	time.Duration
}

type SerializableWeekday struct {
	time.Weekday
}

func (d *SerializableDate) UnmarshalYAML(value *yaml.Node) error {
	date, err := getDateErr(value.Value)
	if err != nil {
		return err
	}
	d.Time = date
	return nil
}

func (d *SerializableDuration) UnmarshalYAML(value *yaml.Node) error {
	pattern := regexp.MustCompile(`^(\d{2}):00`)
	matches := pattern.FindStringSubmatch(value.Value)
	if matches == nil {
		return fmt.Errorf("unable to parse duration: %s", value.Value)
	}
	hours, err := strconv.Atoi(matches[1])
	if err != nil {
		return err
	}
	d.Duration = time.Duration(hours) * time.Hour
	return nil
}

func (d *SerializableWeekday) UnmarshalYAML(value *yaml.Node) error {
	weekday, err := getWeekday(value.Value)
	if err != nil {
		return err
	}
	d.Weekday = weekday
	return nil
}


func getWeekday(weekdayString string) (time.Weekday, error) {
	switch weekdayString {
	case "Sunday":
		return time.Sunday, nil
	case "Monday":
		return time.Monday, nil
	case "Tuesday":
		return time.Tuesday, nil
	case "Wednesday":
		return time.Wednesday, nil
	case "Thursday":
		return time.Thursday, nil
	case "Friday":
		return time.Friday, nil
	case "Saturday":
		return time.Saturday, nil
	default:
		return time.Sunday, fmt.Errorf("invalid weekday string: %s", weekdayString)
	}
}