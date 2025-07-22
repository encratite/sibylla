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
	dataMine := flag.Bool("data-mine", false, "Data mine strategies, supports -from, -to, -assets")
	from := flag.String("from", "", "Restrict data to be read to after this date, used by -data-mine")
	to := flag.String("to", "", "Read no data after this date, used by -data-mine")
	assets := flag.String("assets", "", "Limit data mining to the specified symbols (separated by spaces)")
	flag.Parse()
	if *generateAll {
		sibylla.Generate(nil)
	} else if *generateSymbol != "" {
		sibylla.Generate(generateSymbol)
	} else if *viewArchive != "" {
		sibylla.ViewArchive(*viewArchive)
	} else if dataMine != nil && *dataMine {
		sibylla.DataMine(*from, *to, *assets)
	} else {
		flag.Usage()
	}
}