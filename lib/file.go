package gas

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const indexFile string = ".index.json"
const bucketExt string = ".gas"

type FileService struct {
	path      string   // Will always terminate with a / and be absolute
	indexPath string   // The path to the .index.json file ( convenience )
	Buckets   []string `json:"buckets"`
}

type FileServiceBucket struct {
	name     string
	path     string // Will always terminate with a / and be absolute
	filePath string // Convenience to path/[name].json
	file     *os.File
}

// NewFileService expects one argument:
// 	- path : The absolute or relative path to the directory for storage.
func NewFileService(args ...string) (Service, error) {
	var err error

	if len(args) < 1 {
		return nil, fmt.Errorf("Missing required arguments: path.")
	}

	// Format our path - we want it to be absolute with a trailing /
	path := fmt.Sprintf("%s", args[0])

	path, err = filepath.Abs(path)

	if err != nil {
		return nil, err
	}

	if path[len(path)-1:] != "/" {
		path = path + "/"
	}

	pathInfo, err := os.Stat(path)

	if err != nil {
		return nil, fmt.Errorf("Path not found: %s", path)
	}

	if !pathInfo.IsDir() {
		return nil, fmt.Errorf("Path is not a directory: %s", path)
	}

	s := FileService{
		path:      path,
		indexPath: path + indexFile,
	}

	// Check to see if we already have an index file.
	_, err = os.Stat(path + indexFile)

	if err == nil {
		fileContents, err := ioutil.ReadFile(path + indexFile)

		if err != nil {
			return nil, fmt.Errorf("Error reading index file: %s - %s", path+indexFile, err)
		}

		json.Unmarshal(fileContents, &s)

		return &s, nil
	}

	s.Buckets = make([]string, 0)

	err = s.syncIndex()

	if err != nil {
		return nil, err
	}

	return &s, nil
}

func (s *FileService) syncIndex() error {
	indexJson, err := json.Marshal(&s)

	err = ioutil.WriteFile(s.indexPath, indexJson, 0644)

	return err
}

func (s *FileService) GetBucket(name string) (Bucket, error) {
	var err error

	name = strings.ToLower(name)

	var bucketExists bool = false

	for _, bName := range s.Buckets {
		if name == bName {
			bucketExists = true
		}
	}

	b := FileServiceBucket{
		name:     name,
		path:     s.path,
		filePath: s.path + name + bucketExt,
	}

	if !bucketExists {
		s.Buckets = append(s.Buckets, b.name)
		s.syncIndex()
	}

	// Check if file already exists.
	_, err = os.Stat(b.filePath)

	b.file, err = os.OpenFile(b.filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)

	if err != nil {
		return nil, fmt.Errorf("Could not open file: %s - %s", b.filePath, err)
	}

	b.file.Sync()

	return &b, nil
}

func (b *FileServiceBucket) PushItem(bytes []byte) error {
	var err error

	bytes = append(bytes, []byte("\r\n")...)

	bytes = append([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)+" "), bytes...)

	_, err = b.file.Write(bytes)

	if err != nil {
		return err
	}

	/*
		err = b.file.Sync()

		if err != nil {
			return err
		}
	*/

	return nil
}

func (b *FileServiceBucket) GetLength() (int, error) {
	file, err := os.Open(b.filePath)
	defer file.Close()

	if err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(file)
	n := 0

	for scanner.Scan() {
		n++
	}

	return n, nil
}

func (b *FileServiceBucket) GetName() string {
	return b.name
}

func (b *FileServiceBucket) GetFirstItems(n ...int) ([]Item, error) {
	// Offset & number to retrieve
	offset := 0
	length := 0

	if len(n) > 0 {
		length = n[0]
	}

	if len(n) > 1 {
		offset = n[1]
	}

	items := make([]Item, 0)

	file, err := os.Open(b.filePath)
	defer file.Close()

	if err != nil {
		return items, err
	}

	scanner := bufio.NewScanner(file)
	i := 0

	for scanner.Scan() {
		if i >= offset && i < (offset+length) {
			item, err := parseLineItem(scanner.Text())

			if err != nil {
				return items, err
			}

			items = append(items, item)
		}

		i++
	}

	return items, nil
}

// This is horribly inefficient
// Should be replaced with a custom scanner that goes from the end of a file
// towards the beginning.
// i.e. https://code.google.com/p/rog-go/source/browse/reverse/scan.go
func (b *FileServiceBucket) GetLastItems(n ...int) ([]Item, error) {
	offset := 0
	length := 0

	if len(n) > 0 {
		length = n[0]
	}

	if len(n) > 1 {
		offset = n[1]
	}

	items := make([]Item, 0)

	file, err := os.Open(b.filePath)
	defer file.Close()

	if err != nil {
		return items, err
	}

	// We just create a throwaway scanner to count lines:
	// https://groups.google.com/d/msg/golang-nuts/_eqP4nU4Cjw/wtWj4_S0WJwJ
	lengthScanner := bufio.NewScanner(file)

	l := 0

	for lengthScanner.Scan() {
		l++
	}

	file.Seek(0, 0)

	scanner := bufio.NewScanner(file)

	i := 0

	for scanner.Scan() {
		if i >= (l-(offset+length)) && i < (l-offset) {
			item, err := parseLineItem(scanner.Text())

			if err != nil {
				return items, err
			}

			items = append(items, item)
		}

		i++
	}

	return items, nil
}

func parseLineItem(line string) (Item, error) {
	n := strings.Index(line, " ")

	if n < 0 {
		return Item{}, fmt.Errorf("Invalid line - could not find space separator.")
	}

	timestampString := line[0:n]
	byteString := line[(n + 1):]

	timestamp, err := strconv.ParseInt(timestampString, 10, 64)

	if err != nil {
		return Item{}, err
	}

	return Item{timestamp, []byte(byteString)}, nil
}

func (b *FileServiceBucket) RemoveAllItems() error {
	err := b.file.Truncate(0)

	// The only err that can realistically occur is *PathError -
	// Since we've already validated the path it's highly unlikely to occur. :)
	if err != nil {
		return err
	}

	err = b.file.Sync()

	if err != nil {
		return err
	}

	return nil
}
