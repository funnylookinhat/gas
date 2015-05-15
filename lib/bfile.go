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
	"sync"
	"time"
)

const bfsIndexFile string = ".index.json"
const bfsBucketExt string = ".gas"

type BFileService struct {
	path          string // Will always terminate with a / and be absolute
	indexPath     string // The path to the .index.json file ( convenience )
	initedBuckets map[string]*BFileServiceBucket
	Buckets       []string `json:"buckets"`
}

type BFileServiceBucket struct {
	name         string
	path         string // Will always terminate with a / and be absolute
	filePath     string // Convenience to path/[name].gas
	file         *os.File
	indexPath    string            // path to storage file for byte offsets
	ByteOffsets  []BFileByteOffset `json:"byteoffsets"`
	bfsIndexFile *os.File
	mutex        *sync.Mutex
	length       int64
}

type BFileByteOffset struct {
	Line   int64 `json:"line"`
	Offset int64 `json:"offset"`
}

// NewBFileService expects one argument:
// 	- path : The absolute or relative path to the directory for storage.
func NewBFileService(args ...string) (Service, error) {
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

	s := BFileService{
		path:          path,
		indexPath:     path + bfsIndexFile,
		initedBuckets: make(map[string]*BFileServiceBucket),
	}

	// Check to see if we already have an index file.
	_, err = os.Stat(path + bfsIndexFile)

	if err == nil {
		fileContents, err := ioutil.ReadFile(path + bfsIndexFile)

		if err != nil {
			return nil, fmt.Errorf("Error reading index file: %s - %s", path+bfsIndexFile, err)
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

func (s *BFileService) syncIndex() error {
	indexJson, err := json.Marshal(&s)

	err = ioutil.WriteFile(s.indexPath, indexJson, 0644)

	return err
}

func (s *BFileService) GetBucket(name string) (Bucket, error) {
	var err error

	name = strings.ToLower(name)

	if _, ok := s.initedBuckets[name]; ok {
		return s.initedBuckets[name], nil
	}

	var bucketExists bool = false

	for _, bName := range s.Buckets {
		if name == bName {
			bucketExists = true
		}
	}

	b := BFileServiceBucket{
		name:      name,
		path:      s.path,
		filePath:  s.path + name + bfsBucketExt,
		indexPath: s.path + name + bfsBucketExt + bfsIndexFile,
		mutex:     &sync.Mutex{},
	}

	if !bucketExists {
		s.Buckets = append(s.Buckets, b.name)
		s.syncIndex()
	}

	b.file, err = os.OpenFile(b.filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)

	if err != nil {
		return nil, fmt.Errorf("Could not open file: %s - %s", b.filePath, err)
	}

	b.file.Sync()

	length, err := b.GetLength()

	if err != nil {
		return nil, fmt.Errorf("Could not initialize Bucket - cannot get length! %s", err)
	}

	b.length = int64(length)

	_, err = os.Stat(b.filePath + bfsIndexFile)

	if err == nil {
		fileContents, err := ioutil.ReadFile(b.filePath + bfsIndexFile)

		if err != nil {
			return nil, fmt.Errorf("Error reading index file: %s - %s", b.filePath+bfsIndexFile, err)
		}

		json.Unmarshal(fileContents, &b)
	} else {
		b.ByteOffsets = make([]BFileByteOffset, 0)
		b.syncIndex()
	}

	s.initedBuckets[b.name] = &b

	return &b, nil
}

func (b *BFileServiceBucket) syncIndex() error {
	indexJson, err := json.Marshal(&b)

	err = ioutil.WriteFile(b.filePath+bfsIndexFile, indexJson, 0644)

	return err
}

func (b *BFileServiceBucket) PushItem(bytes []byte) error {
	var err error

	bytes = append(bytes, []byte("\r\n")...)

	bytes = append([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)+" "), bytes...)

	b.mutex.Lock()

	_, err = b.file.Write(bytes)

	b.length++

	if b.length%10000 == 0 {
		offset, err := b.file.Seek(0, 1)
		if err == nil {
			b.ByteOffsets = append(b.ByteOffsets, BFileByteOffset{Line: b.length, Offset: offset})
			b.syncIndex()
		}
	}

	b.mutex.Unlock()

	if err != nil {
		return err
	}

	return nil
}

func (b *BFileServiceBucket) GetLength() (int, error) {
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

func (b *BFileServiceBucket) GetName() string {
	return b.name
}

func (b *BFileServiceBucket) GetFirstItems(n ...int) ([]Item, error) {
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

	fileOffset := int64(0)
	lineOffset := int64(0)

	for _, byteOffset := range b.ByteOffsets {
		if byteOffset.Line < int64(offset) {
			fileOffset = int64(byteOffset.Offset)
			lineOffset = int64(byteOffset.Line)
		} else {
			break
		}
	}

	_, err = file.Seek(fileOffset, 0)

	if err != nil {
		return items, err
	}

	scanner := bufio.NewScanner(file)
	i := lineOffset

	for scanner.Scan() {
		if i >= int64(offset) && i < (int64(offset)+int64(length)) {
			item, err := parseLineItem(scanner.Text())

			if err != nil {
				return items, err
			}

			items = append(items, item)
		} else if i >= (int64(offset) + int64(length)) {
			break
		}

		i++
	}

	return items, nil
}

// This is horribly inefficient
// Should be replaced with a custom scanner that goes from the end of a file
// towards the beginning.
// i.e. https://code.google.com/p/rog-go/source/browse/reverse/scan.go
func (b *BFileServiceBucket) GetLastItems(n ...int) ([]Item, error) {
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

	l := b.length

	fileOffset := int64(0)
	lineOffset := int64(0)

	for _, byteOffset := range b.ByteOffsets {
		if byteOffset.Line < int64((l - (int64(offset) + int64(length)))) {
			fileOffset = int64(byteOffset.Offset)
			lineOffset = int64(byteOffset.Line)
		} else {
			break
		}
	}

	_, err = file.Seek(fileOffset, 0)

	if err != nil {
		return items, err
	}

	scanner := bufio.NewScanner(file)
	i := lineOffset

	for scanner.Scan() {
		if i >= int64((l-(int64(offset)+int64(length)))) && i < int64(int64(l)-int64(offset)) {
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

/*
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
*/

func (b *BFileServiceBucket) RemoveAllItems() error {
	err := b.file.Truncate(0)

	b.ByteOffsets = make([]BFileByteOffset, 0)
	b.syncIndex()

	// The only err that can realistically occur is *PathError -
	// Since we've already validated the path it's highly unlikely to occur. :)
	if err != nil {
		return err
	}

	err = b.file.Sync()

	b.length = 0

	if err != nil {
		return err
	}

	return nil
}
