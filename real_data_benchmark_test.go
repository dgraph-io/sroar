package roar

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
)

// To run these benchmarks: go test -bench BenchmarkRealDataFastOr -run -

var realDatasets = []string{
	"census-income_srt", "census-income", "census1881_srt", "census1881",
	"dimension_003", "dimension_008", "dimension_033", "uscensus2000", "weather_sept_85_srt",
	"weather_sept_85", "wikileaks-noquotes_srt", "wikileaks-noquotes",
}

func retrieveRealDataBitmaps(datasetName string, optimize bool) ([]*Bitmap, error) {
	gopath, ok := os.LookupEnv("GOPATH")
	if !ok {
		return nil, fmt.Errorf("GOPATH not set. It's required to locate real-roaring-dataset.")
	}

	basePath := path.Join(gopath, "src", "github.com", "RoaringBitmap", "real-roaring-datasets")

	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("real-roaring-datasets does not exist. " +
			"Run `go get github.com/RoaringBitmap/real-roaring-datasets`")
	}

	datasetPath := path.Join(basePath, datasetName+".zip")

	if _, err := os.Stat(datasetPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("dataset %s does not exist, tried path: %s",
			datasetName, datasetPath)
	}

	zipFile, err := zip.OpenReader(datasetPath)
	if err != nil {
		return nil, fmt.Errorf("error opening dataset %s zipfile, cause: %v", datasetPath, err)
	}
	defer zipFile.Close()

	var largestFileSize uint64
	for _, f := range zipFile.File {
		if f.UncompressedSize64 > largestFileSize {
			largestFileSize = f.UncompressedSize64
		}
	}

	bitmaps := make([]*Bitmap, len(zipFile.File))
	buf := make([]byte, largestFileSize)
	var bufStep uint64 = 32768 // apparently the largest buffer zip can read
	for i, f := range zipFile.File {
		r, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to read bitmap file %s from dataset %s, cause: %v",
				f.Name, datasetName, err)
		}

		var totalReadBytes uint64

		for {
			var endOffset uint64
			if f.UncompressedSize64 < totalReadBytes+bufStep {
				endOffset = f.UncompressedSize64
			} else {
				endOffset = totalReadBytes + bufStep
			}

			readBytes, err := r.Read(buf[totalReadBytes:endOffset])
			totalReadBytes += uint64(readBytes)

			if err == io.EOF {
				r.Close()
				break
			} else if err != nil {
				r.Close()
				return nil, fmt.Errorf("could not read content of file %s from dataset %s, err: %v",
					f.Name, datasetName, err)
			}
		}

		elemsAsBytes := bytes.Split(buf[:totalReadBytes], []byte{44}) // 44 is a comma

		b := NewBitmap()
		for _, elemBytes := range elemsAsBytes {
			elemStr := strings.TrimSpace(string(elemBytes))

			e, err := strconv.ParseUint(elemStr, 10, 32)
			if err != nil {
				r.Close()
				return nil, fmt.Errorf("could not parse %s as uint32. Reading %s from %s. err: %v",
					elemStr, f.Name, datasetName, err)
			}

			b.Set(uint64(e))
		}

		// if optimize {
		// 	b.RunOptimize()
		// }

		bitmaps[i] = b
	}

	return bitmaps, nil
}

func benchmarkRealDataAggregate(b *testing.B, aggregator func(b []*Bitmap) int) {
	for _, dataset := range realDatasets {
		b.Run(dataset, func(b *testing.B) {
			bitmaps, err := retrieveRealDataBitmaps(dataset, true)
			if err != nil {
				b.Fatal(err)
			}
			c := aggregator(bitmaps)
			b.Logf("Got cardinality: %d\n", c)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				aggregator(bitmaps)
			}
		})
	}
}

func BenchmarkRealDataFastOr(b *testing.B) {
	benchmarkRealDataAggregate(b, func(bitmaps []*Bitmap) int {
		return FastOr(bitmaps...).GetCardinality()
	})
}
