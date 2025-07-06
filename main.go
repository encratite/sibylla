package main

import (
	"flag"
	"sibylla/sibylla"
)

func main() {
	generate := flag.Bool("generate", true, "Generate .gob files from Barchart .csv files")
	flag.Parse()
	if *generate {
		sibylla.Generate()
	} else {
		flag.Usage()
	}
}
