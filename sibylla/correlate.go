package sibylla

import (
	"fmt"
	"log"
	"time"
)

type assetPath struct {
	asset Asset
	path string
}

type assetArchive struct {
	asset Asset
	archive Archive
}

func Correlate(fromString, toString string) {
	from := getDateFromArg(fromString)
	to := getDateFromArg(toString)
	assetPaths := []assetPath{}
	for _, asset := range *assets {
		fRecords := 1
		if asset.FRecords != nil {
			fRecords = *asset.FRecords
		}
		for fNumber := 1; fNumber <= fRecords; fNumber++ {
			path := getArchivePath(asset.Symbol, fNumber)
			ap := assetPath{
				asset: asset,
				path: path,
			}
			assetPaths = append(assetPaths, ap)
		}
	}
	start := time.Now()
	assetArchives := parallelMap(assetPaths, func (ap assetPath) assetArchive {
		archive := readArchive(ap.path)
		return assetArchive{
			asset: ap.asset,
			archive: archive,
		}
	})
	delta := time.Since(start)
	fmt.Printf("Loaded archives in %.2f s\n", delta.Seconds())
	fmt.Println(from)
	fmt.Println(to)
	fmt.Println(assetArchives)
}

func getDateFromArg(argument string) *time.Time {
	if argument != "" {
		date, err := getDate(argument)
		if err != nil {
			log.Fatal(err)
		}
		return &date
	} else {
		return nil
	}
}

