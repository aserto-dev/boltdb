package boltdb

import "github.com/pkg/errors"

var (
	ErrPathNotFound = errors.New("path not found")
	ErrKeyNotFound  = errors.New("key not found")
	ErrKeyExists    = errors.New("key already exists")
)
