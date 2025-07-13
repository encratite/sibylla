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
	validateArchive := flag.String("validate", "", "Load the archive file with the specified symbol and statistically analyze its contents to make sure that the data are consistent")
	flag.Parse()
	if generate != nil && *generate {
		sibylla.Generate()
	} else if validateArchive != nil && *validateArchive != "" {
		sibylla.ValidateArchive(*validateArchive)
	} else {
		flag.Usage()
	}
}
