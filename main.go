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
	correlate := flag.Bool("correlate", false, "Load archives and correlate features")
	from := flag.String("from", "", "Restrict data to be read to after this date, used by -correlate")
	to := flag.String("to", "", "Read no data after this date, used by -correlate")
	flag.Parse()
	if *generateAll {
		sibylla.Generate(nil)
	} else if *generateSymbol != "" {
		sibylla.Generate(generateSymbol)
	} else if *viewArchive != "" {
		sibylla.ViewArchive(*viewArchive)
	} else if correlate != nil && *correlate {
		sibylla.Correlate(*from, *to)
	} else {
		flag.Usage()
	}
}