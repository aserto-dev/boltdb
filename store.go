package boltdb

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Store struct {
	logger *zerolog.Logger
	config *Config
	db     *bolt.DB
}

func NewStore(cfg *Config, logger *zerolog.Logger) *Store {
	newLogger := logger.With().Str("component", "store").Logger()

	return &Store{
		config: cfg,
		logger: &newLogger,
		db:     nil,
	}
}

// Open store.
func (s *Store) Open() error {
	s.logger.Info().Str("DBPath", s.config.DBPath).Msg("open::boltdb")
	var err error

	if s.config.DBPath == "" {
		return errors.New("store path not set")
	}

	dbDir := filepath.Dir(s.config.DBPath)
	exists, err := filePathExists(dbDir)
	if err != nil {
		return errors.Wrap(err, "failed to determine if store path/file exists")
	}
	if !exists {
		if err = os.MkdirAll(dbDir, 0700); err != nil {
			return errors.Wrapf(err, "failed to create directory '%s'", dbDir)
		}
	}

	db, err := bolt.Open(s.config.DBPath, 0600, &bolt.Options{Timeout: s.config.RequestTimeout})
	if err != nil {
		return errors.Wrapf(err, "failed to open directory '%s'", s.config.DBPath)
	}

	s.db = db

	return nil
}

// Close store
func (s *Store) Close() {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

// Start new read session.
func (s *Store) ReadSession() (*Session, func(), error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to start read transaction")
	}

	session := Session{
		store: s,
		tx:    tx,
	}

	closer := func() {
		_ = session.tx.Rollback()
	}

	return &session, closer, nil
}

// Start new write session
func (s *Store) WriteSession() (*Session, func(), error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to start write transaction")
	}

	session := Session{
		store: s,
		tx:    tx,
	}

	closer := func() {
		if session.err != nil {
			_ = session.tx.Rollback()
			return
		}
		_ = session.tx.Commit()
	}

	return &session, closer, nil
}

// filePathExists, internal helper function to detect if the file path exists
func filePathExists(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, errors.Wrapf(err, "failed to stat file [%s]", path)
	}
}
