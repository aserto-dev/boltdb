package boltdb_test

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/aserto-dev/boltdb"
	"github.com/rs/zerolog"
)

var (
	cfg *boltdb.Config
)

func TestMain(m *testing.M) {
	tmpDir, err := ioutil.TempDir("", "tests")
	if err != nil {
		return
	}

	cfg = &boltdb.Config{}
	cfg.DBPath = filepath.Join(tmpDir, "test.db")

	os.Exit(m.Run())
}

func setupStore(t *testing.T) *boltdb.Store {
	logger := zerolog.New(io.Discard)

	store := boltdb.NewStore(cfg, &logger)

	if err := store.Open(); err != nil {
		t.Logf("Open %v", err)
		t.FailNow()
	}

	return store
}
