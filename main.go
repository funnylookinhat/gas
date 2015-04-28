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
			testServiceSpeed(fileService, length, randomData[s])
		}
	}

	fileTestDuration := time.Since(fileTestStartTime)

	fmt.Printf("All file tests ( including random data generation ): %s\n", fileTestDuration)

}

// n = number of items in a bucket
// s = size of item data in bytes
func testServiceSpeed(service gas.Service, n int, randomData [][]byte) {
	var err error

	s := len(randomData[0])

	b, err := service.GetBucket("test1")

	if err != nil {
		log.Fatal(err)
	}

	b.RemoveAllItems()

	r := n / 10

	// Can't make the test too easy...
	if r < 5 {
		r = 5
	}

	// But, we don't want to spend all day.
	if r > 2000 {
		r = 2000
	}

	h := n / 2

	if h < 1 {
		h = 1
	}

	fmt.Printf("Pushing %d items (%d bytes) to %s -> ", n, s, b.GetName())
	startTime := time.Now()
	startAllTime := startTime
	for i := 0; i < n; i++ {
		b.PushItem(randomData[rand.Intn(len(randomData))])
	}
	duration := time.Since(startTime)
	fmt.Printf("Result: %s \n", duration)

	fmt.Printf("Fetching %d first items -> ", r)
	startTime = time.Now()
	_, _ = b.GetFirstItems(r)
	duration = time.Since(startTime)
	fmt.Printf("Result: %s \n", duration)

	fmt.Printf("Fetching %d first items (offset %d) -> ", r, h)
	startTime = time.Now()
	_, _ = b.GetFirstItems(r, h)
	duration = time.Since(startTime)
	fmt.Printf("Result: %s \n", duration)

	fmt.Printf("Fetching %d last items -> ", r)
	startTime = time.Now()
	_, _ = b.GetLastItems(r)
	duration = time.Since(startTime)
	fmt.Printf("Result: %s \n", duration)

	fmt.Printf("Fetching %d last items (offset %d) -> ", r, h)
	startTime = time.Now()
	_, _ = b.GetLastItems(r, h)
	duration = time.Since(startTime)
	fmt.Printf("Result: %s \n", duration)

	duration = time.Since(startAllTime)

	fmt.Printf("Total time for %d items (%d bytes) : %s\n\n", n, s, duration)

}

func generateBytes(length int) []byte {
	charset := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	bytes := ""
	for i := 0; i < length; i++ {
		bytes = bytes + string(charset[rand.Intn(len(charset))])
	}

	return []byte(bytes)
}
