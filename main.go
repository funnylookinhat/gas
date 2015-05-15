package main

import (
	"flag"
	"fmt"
	"github.com/funnylookinhat/gas/lib"
	"log"
	"math/rand"
	"os"
	"time"
)

type Benchmark struct {
	service       gas.Service
	description   string
	tests         []BenchmarkTest
	writeDuration time.Duration
	readDuration  time.Duration
}

type BenchmarkTest struct {
	bytes         int
	length        int
	writeDuration time.Duration
	readDuration  time.Duration
}

// Make sure you've got at least 5 GBs of free space before running this.
// Also - please ask your HDD for forgiveness.
func main() {

	var quick string
	flag.StringVar(&quick, "quick", "", "Whether or not to run a faster set of tests.")
	flag.Parse()

	smallByteSizes := []int{128, 256, 512}
	smallByteLengths := []int{1000, 5000, 10000, 50000, 100000, 500000}

	largeByteSizes := []int{1024, 2048, 4096}
	largeByteLengths := []int{1000, 5000, 10000}

	// Temporary overrides
	if quick == "yes" {
		fmt.Printf("Running shortened test suite ...\n")
		smallByteSizes = []int{128, 256, 512}
		smallByteLengths = []int{1000, 5000, 10000}
		largeByteSizes = []int{1024}
		largeByteLengths = []int{1000}
	} else {
		fmt.Printf("Running full test suite - to check only a subset of tests run with --quick=yes\n")
	}

	_ = os.MkdirAll("gastest/file", 0755)
	_ = os.MkdirAll("gastest/bfile", 0755)

	benchmarks := make([]Benchmark, 0)

	fileService, err := gas.NewService("file", []string{"gastest/file/"}...)

	if err != nil {
		log.Fatalf("Error creating file service: %s", err)
	}

	benchmarks = append(benchmarks, Benchmark{
		service:     fileService,
		description: "File",
		tests:       make([]BenchmarkTest, 0),
	})

	bfileService, err := gas.NewService("bfile", []string{"gastest/bfile/"}...)

	if err != nil {
		log.Fatalf("Error creating bfile service: %s", err)
	}

	benchmarks = append(benchmarks, Benchmark{
		service:     bfileService,
		description: "BFile",
		tests:       make([]BenchmarkTest, 0),
	})

	for i, _ := range benchmarks {
		// Small byte tests up to 10,000,000 items.
		for _, s := range smallByteSizes {
			for _, l := range smallByteLengths {
				benchmarks[i].tests = append(benchmarks[i].tests, BenchmarkTest{
					bytes:  s,
					length: l,
				})
			}
		}

		// Large byte tests up to 100,000 items.
		for _, s := range largeByteSizes {
			for _, l := range largeByteLengths {
				benchmarks[i].tests = append(benchmarks[i].tests, BenchmarkTest{
					bytes:  s,
					length: l,
				})
			}
		}
	}

	fmt.Printf("Generating random data... ")

	randomData := make(map[int][][]byte)

	for _, s := range []int{128, 256, 512, 1024, 2048, 4096} {
		fmt.Printf("%d ... ", s)
		randomData[s] = make([][]byte, 0)
		for i := 0; i < 100; i++ {
			randomData[s] = append(randomData[s], generateBytes(s))
		}
	}

	fmt.Printf("done!\n")

	fmt.Printf("Running tests...\n")

	for i, b := range benchmarks {
		fmt.Printf("\nRunning Service Tests: %s\n", b.description)

		benchmarks[i].readDuration = time.Since(time.Now())
		benchmarks[i].writeDuration = time.Since(time.Now())

		for j, t := range b.tests {
			fmt.Printf("  -> %d Lines @ %d Bytes ... ", t.length, t.bytes)
			w, r, err := testServiceSpeed(b.service, t.length, randomData[t.bytes])
			if err != nil {
				log.Fatal(err)
			}
			benchmarks[i].tests[j].readDuration = r
			benchmarks[i].tests[j].writeDuration = w

			benchmarks[i].readDuration += r
			benchmarks[i].writeDuration += w

			fmt.Printf("write: %s , read: %s", benchmarks[i].tests[j].writeDuration, benchmarks[i].tests[j].readDuration)

			fmt.Printf("\n")
		}
	}

	fmt.Printf("\n")

	for _, b := range benchmarks {
		fmt.Printf("%s Total write: %s , read: %s \n", b.description, b.writeDuration, b.readDuration)
	}

	return
}

// n = number of items in a bucket
// s = size of item data in bytes
func testServiceSpeed(service gas.Service, n int, randomData [][]byte) (writeDuration, readDuration time.Duration, err error) {
	if n < 500 {
		err = fmt.Errorf("That test isn't worth running: n must be >= 500.")
		return
	}

	// s := len(randomData[0])

	b, err := service.GetBucket("test1")

	if err != nil {
		return
	}

	b.RemoveAllItems()

	// r = Number of items we will fetch at a time in read tests.
	r := n / 10

	if r > 500 {
		r = 500
	}

	// Number of fetch tests to run ( x2 - first and last )
	q := 1000

	startWriteTime := time.Now()
	for i := 0; i < n; i++ {
		b.PushItem(randomData[rand.Intn(len(randomData))])
	}
	writeDuration = time.Since(startWriteTime)

	startReadTime := time.Now()
	for i := 0; i < q; i++ {
		// Offset
		o := i * (n / q)
		_, _ = b.GetFirstItems(r, o)
		_, _ = b.GetLastItems(r, o)
	}

	readDuration = time.Since(startReadTime)

	b.RemoveAllItems()

	return
}

func generateBytes(length int) []byte {
	charset := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	bytes := ""
	for i := 0; i < length; i++ {
		bytes = bytes + string(charset[rand.Intn(len(charset))])
	}

	return []byte(bytes)
}
