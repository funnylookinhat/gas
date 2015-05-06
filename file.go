package main

import (
	"log"
	"os"
)

func main() {
	size := int64(1000 * 1024 * 1024)
	f, err := os.Create("test/test1.gas")
	if err != nil {
		log.Fatal("Failed to create test/testfile")
	}
	if err := f.Truncate(size); err != nil {
		log.Fatal(err)
	}
}
