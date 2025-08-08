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

type SerializableSide struct {
	PositionSide
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

func (s *SerializableSide) UnmarshalYAML(value *yaml.Node) error {
	switch value.Value {
	case "long":
		s.PositionSide = SideLong
	case "short":
		s.PositionSide = SideShort
	default:
		return fmt.Errorf("invalid position side string \"%s\"", value.Value)
	}
	return nil
}

func (w *SerializableWeekday) UnmarshalYAML(value *yaml.Node) error {
	switch value.Value {
	case "Monday":
		w.Weekday = time.Monday
	case "Tuesday":
		w.Weekday = time.Tuesday
	case "Wednesday":
		w.Weekday = time.Wednesday
	case "Thursday":
		w.Weekday = time.Thursday
	case "Friday":
		w.Weekday = time.Friday
	default:
		return fmt.Errorf("invalid weekday string \"%s\"", value.Value)
	}
	return nil
}