package sibylla

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func GenerateStrategyYaml(inputPath, outputPath string) {
	bytes, err := os.ReadFile(inputPath)
	if err != nil {
		log.Fatalf("Failed to read file")
	}
	content := string(bytes)
	content = strings.ReplaceAll(content, "\r", "")
	lines := strings.Split(content, "\n")
	output := "strategies:\n"
	
	for _, line := range lines {
		if line == "" {
			continue
		}
		if generateTwoFeatureStrategy(line, &output) {
			continue
		}
		if generateSingleFeatureStrategy(line, &output) {
			continue
		}
		log.Fatalf("Unable to parse line: %s", line)
	}
	err = os.WriteFile(outputPath, []byte(output), 0644)
	if err != nil {
		log.Fatal("Failed to write file")
	}
}

func generateTwoFeatureStrategy(line string, output *string) bool {
	pattern := regexp.MustCompile(`^(.+?)\.(momentum.+?) \((\d+(?:\.\d+)?), (\d+(?:\.\d+)?)\), (.+?)\.(momentum.+?) \((\d+(?:\.\d+)?), (\d+(?:\.\d+)?)\), (long|short), (\d+:\d+), (\d+)h(?:, SL (\d+\.\d+)%)?$`)
	matches := pattern.FindStringSubmatch(line)
	if matches == nil {
		return false
	}
	symbol1 := matches[1]
	feature1 := matches[2]
	min1 := matches[3]
	max1 := matches[4]
	symbol2 := matches[5]
	feature2 := matches[6]
	min2 := matches[7]
	max2 := matches[8]
	side := matches[9]
	time := matches[10]
	holdingTime := matches[11]
	stopLoss := getStopLossFromString(matches[12])
	*output += fmt.Sprintf("  - symbol: %s\n", symbol1)
	*output += fmt.Sprintf("    side: %s\n", side)
	*output += fmt.Sprintf("    time: %s\n", time)
	*output += fmt.Sprintf("    holdingTime: %s\n", holdingTime)
	if stopLoss != nil {
		*output += fmt.Sprintf("    stopLoss: %.3f\n", *stopLoss)
	}
	*output += "    conditions:\n"
	*output += fmt.Sprintf("      - feature: %s\n", feature1)
	*output += fmt.Sprintf("        min: %s\n", min1)
	*output += fmt.Sprintf("        max: %s\n", max1)
	*output += fmt.Sprintf("      - symbol: %s\n", symbol2)
	*output += fmt.Sprintf("        feature: %s\n", feature2)
	*output += fmt.Sprintf("        min: %s\n", min2)
	*output += fmt.Sprintf("        max: %s\n", max2)
	return true
}

func generateSingleFeatureStrategy(line string, output *string) bool {
	pattern := regexp.MustCompile(`^(.+?)\.(momentum.+?) \((\d+(?:\.\d+)?), (\d+(?:\.\d+)?)\), (long|short), (\d+:\d+), (\d+)h(?:, SL (\d+\.\d+)%)?$`)
	matches := pattern.FindStringSubmatch(line)
	if matches == nil {
		return false
	}
	symbol := matches[1]
	feature := matches[2]
	min := matches[3]
	max := matches[4]
	side := matches[5]
	time := matches[6]
	holdingTime := matches[7]
	stopLoss := getStopLossFromString(matches[8])
	*output += fmt.Sprintf("  - symbol: %s\n", symbol)
	*output += fmt.Sprintf("    side: %s\n", side)
	*output += fmt.Sprintf("    time: %s\n", time)
	*output += fmt.Sprintf("    holdingTime: %s\n", holdingTime)
	if stopLoss != nil {
		*output += fmt.Sprintf("    stopLoss: %.3f\n", *stopLoss)
	}
	*output += "    conditions:\n"
	*output += fmt.Sprintf("      - feature: %s\n", feature)
	*output += fmt.Sprintf("        min: %s\n", min)
	*output += fmt.Sprintf("        max: %s\n", max)
	return true
}

func getStopLossFromString(input string) *float64 {
	if input == "" {
		return nil
	}
	value, err := strconv.ParseFloat(input, 64)
	if err != nil {
		log.Fatal("Failed to parse SL value")
	}
	value /= 100.0
	return &value
}