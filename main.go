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
	generate := flag.String("generate", "", "Generate .gob archive for just that symbol")
	viewArchive := flag.String("archive", "", "Analyze archive contents of the specified symbol")
	flag.Parse()
	if generateAll != nil && *generateAll {
		sibylla.Generate(nil)
	} else if generate != nil {
		sibylla.Generate(generate)
	} else if viewArchive != nil && *viewArchive != "" {
		sibylla.ViewArchive(*viewArchive)
	} else {
		flag.Usage()
	}
}
