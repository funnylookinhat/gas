package gas

import (
	"fmt"
)

type Service interface {
	GetBucket(string) (Bucket, error)
}

type Bucket interface {
	PushItem([]byte) error
	GetLength() (int, error)
	GetName() string
	GetFirstItems(n ...int) ([]Item, error)
	GetLastItems(n ...int) ([]Item, error)
	RemoveAllItems() error
}

type Item struct {
	Timestamp int64
	Data      []byte
}

func NewService(s string, args ...string) (Service, error) {
	if s == "file" {
		return NewFileService(args...)
	}

	return nil, fmt.Errorf("Service not recognized: %s", s)
}
