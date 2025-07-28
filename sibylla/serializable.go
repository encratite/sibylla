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