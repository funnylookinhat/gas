package main

import (
	"./lib"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Make sure you've got at least 5 GBs of free space before running this.
// Also - please ask your HDD for forgiveness.
func main() {

	fmt.Printf("Generating random data... ")

	randomData := make(map[int][][]byte)

	testSizes := []int{512, 1024, 2048, 4096}
	testLengths := []int{1000, 5000, 10000, 50000, 100000, 500000, 1000000}

	// Temporary Override
	testSizes = []int{128} //, 256, 512}
	testLengths = []int{1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000}

	for _, s := range testSizes {
		fmt.Printf("%d ... ", s)
		randomData[s] = make([][]byte, 0)
		for i := 0; i < 100; i++ {
			randomData[s] = append(randomData[s], generateBytes(s))
		}
	}

	fmt.Printf("done!\n")

	fileTestStartTime := time.Now()

	fileService, err := gas.NewService("file", []string{"test/"}...)

	if err != nil {
		log.Fatal(err)
	}

	for _, s := range testSizes {
		for _, length := range testLengths {
			writeDuration, readDuration := testServiceSpeed(fileService, length, randomData[s])
			fmt.Printf("\n- - - - - - - - - - - - - - - - - - - - - - - - - - - - \n")
			fmt.Printf("File service: %d items @ %d bytes -> %s / %s \n\n", length, s, writeDuration, readDuration)
		}
	}

	fileTestDuration := time.Since(fileTestStartTime)

	fmt.Printf("* * * * * * * * * * * * * \n\n\n\n")
	fmt.Printf("All file tests ( including random data generation ): %s\n", fileTestDuration)
	fmt.Printf("\n\n\n* * * * * * * * * * * * * \n")

	fileServiceBucket, err := fileService.GetBucket("test1")

	if err != nil {
		log.Fatal(err)
	}

	fileServiceBucket.RemoveAllItems()

	bfileTestStartTime := time.Now()

	bfileService, err := gas.NewService("bfile", []string{"test/"}...)

	if err != nil {
		log.Fatal(err)
	}

	for _, s := range testSizes {
		for _, length := range testLengths {
			writeDuration, readDuration := testServiceSpeed(bfileService, length, randomData[s])
			fmt.Printf("\n- - - - - - - - - - - - - - - - - - - - - - - - - - - - \n")
			fmt.Printf("BFile service: %d items @ %d bytes -> %s / %s \n\n", length, s, writeDuration, readDuration)
		}
	}

	bfileTestDuration := time.Since(bfileTestStartTime)

	fmt.Printf("* * * * * * * * * * * * * \n\n\n\n")
	fmt.Printf("All bfile tests ( including random data generation ): %s\n", bfileTestDuration)
	fmt.Printf("\n\n\n* * * * * * * * * * * * * \n")

	bfileServiceBucket, err := bfileService.GetBucket("test1")

	if err != nil {
		log.Fatal(err)
	}

	bfileServiceBucket.RemoveAllItems()

}

// n = number of items in a bucket
// s = size of item data in bytes
func testServiceSpeed(service gas.Service, n int, randomData [][]byte) (writeDuration, readDuration time.Duration) {
	var err error

	if n < 500 {
		log.Fatal("That test isn't worth running: n must be >= 500.")
	}

	s := len(randomData[0])

	b, err := service.GetBucket("test1")

	if err != nil {
		log.Fatal(err)
	}

	b.RemoveAllItems()

	// r = Number of items we will fetch at a time in read tests.
	r := n / 10

	if r > 2000 {
		r = 2000
	}

	// Number of fetch tests to run ( x2 - first and last )
	q := n / r

	if q < 1 {
		q = 1
	}

	if q > 20 {
		q = 20
	}

	fmt.Printf("Pushing %d items (%d bytes) to %s -> ", n, s, b.GetName())
	startTime := time.Now()
	startAllTime := startTime
	for i := 0; i < n; i++ {
		b.PushItem(randomData[rand.Intn(len(randomData))])
	}
	duration := time.Since(startTime)
	writeDuration = duration
	fmt.Printf("Result: %s \n", duration)

	// startAllReadTime := time.Now()

	for i := 0; i < q; i++ {
		// Offset
		o := i * (n / q)

		//fmt.Printf("%d Items @ %d Bytes / Fetching %d first items (offset %d) -> ", n, s, r, o)
		//startTime = time.Now()
		_, _ = b.GetFirstItems(r, o)
		//duration = time.Since(startTime)
		//fmt.Printf("Result: %s \n", duration)

		//fmt.Printf("%d Items @ %d Bytes / Fetching %d  last items (offset %d) -> ", n, s, r, o)
		//startTime = time.Now()
		_, _ = b.GetLastItems(r, o)
		//duration = time.Since(startTime)
		//fmt.Printf("Result: %s \n", duration)
	}

	// readDuration = time.Since(startAllReadTime)

	duration = time.Since(startAllTime)

	fmt.Printf("Total time for %d items (%d bytes) : %s\n\n", n, s, duration)

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
