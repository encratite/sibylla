package main

import (
	"flag"
	"fmt"
	"runtime/debug"
	"sibylla/sibylla"
)

func main() {
	defer func() {
		err := recover()
		if err != nil {
			fmt.Println("Panic:", err)
			debug.PrintStack()
		}
	}()
	generateAll := flag.Bool("generate-all", false, "Generate .gob archive files from Barchart .csv files for all assets")
	generateSymbol := flag.String("generate", "", "Generate .gob archive for just that symbol")
	viewArchive := flag.String("archive", "", "Analyze archive contents of the specified symbol")
	dataMine := flag.String("data-mine", "", "Data mine strategies using the parameters from the specified YAML file")
	flag.Parse()
	if *generateAll {
		sibylla.Generate(nil)
	} else if *generateSymbol != "" {
		sibylla.Generate(generateSymbol)
	} else if *viewArchive != "" {
		sibylla.ViewArchive(*viewArchive)
	} else if *dataMine != "" {
		sibylla.DataMine(*dataMine)
	} else {
		flag.Usage()
	}
}