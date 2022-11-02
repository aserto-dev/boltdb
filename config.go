package boltdb

import "time"

const pageSize int32 = 100

type Config struct {
	DBPath         string        `json:"db_path"`
	RequestTimeout time.Duration `json:"request_timeout_in_seconds"`
}
