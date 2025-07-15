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
	generate := flag.Bool("generate", false, "Generate .gob archive files from Barchart .csv files")
	viewArchive := flag.String("archive", "", "Analyze archive contents of the specified symbol")
	flag.Parse()
	if generate != nil && *generate {
		sibylla.Generate()
	} else if viewArchive != nil && *viewArchive != "" {
		sibylla.ViewArchive(*viewArchive)
	} else {
		flag.Usage()
	}
}
