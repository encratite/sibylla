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
	generate := flag.Bool("generate", true, "Generate .gob files from Barchart .csv files")
	flag.Parse()
	if *generate {
		sibylla.Generate()
	} else {
		flag.Usage()
	}
}
