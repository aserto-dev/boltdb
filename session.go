package boltdb

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

type Session struct {
	store *Store   // store pointer
	tx    *bolt.Tx // session transaction
	err   error    // session error
}

// Read value from key in bucket path.
func (s *Session) Read(path []string, key string) ([]byte, error) {
	s.store.logger.Trace().Interface("path", path).Str("key", key).Msg("Session::Read")

	var result []byte

	read := func(tx *bolt.Tx) error {
		b, err := s.setBucket(path)
		if err != nil {
			return errors.Wrapf(ErrPathNotFound, "path [%s]", path)
		}

		result = b.Get([]byte(key))
		if result == nil {
			return errors.Wrapf(ErrKeyNotFound, "key [%s]", key)
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.View(read)
	} else {
		err = read(s.tx)
		s.err = err
	}

	return result, err
}

// List returns paged collection of key and value arrays
func (s *Session) List(path []string, pageToken string) ([]string, [][]byte, string, error) {
	s.store.logger.Trace().Interface("path", path).Str("pageToken", pageToken).Msg("Session::List")

	var (
		keys      = make([]string, 0)
		values    = make([][]byte, 0)
		nextToken string
	)

	list := func(tx *bolt.Tx) error {
		b, err := s.setBucket(path)
		if err != nil {
			return err
		}

		cursor := b.Cursor()

		var k, v []byte
		for i := int32(0); i < pageSize; i++ {
			if i == 0 {
				if pageToken == "" {
					k, v = cursor.First()
				} else {
					k, v = cursor.Seek([]byte(pageToken))
				}
			} else {
				k, v = cursor.Next()
			}
			if k == nil {
				break
			}

			keys = append(keys, string(k))
			values = append(values, v)
		}

		k, _ = cursor.Next()
		if k != nil {
			nextToken = string(k)
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.View(list)
	} else {
		err = list(s.tx)
		s.err = err
	}

	if err != nil {
		s.store.logger.Trace().Err(err).Msg("List")
		return []string{}, [][]byte{}, "", nil
	}

	return keys, values, nextToken, nil
}

// Key exists checks if a key exists at given bucket path.
func (s *Session) KeyExists(path []string, key string) bool {
	s.store.logger.Trace().Interface("path", path).Str("key", key).Msg("Session::KeyExists")

	exists := func(tx *bolt.Tx) error {
		b, err := s.setBucket(path)
		if err != nil {
			return errors.Wrapf(err, "KeyExist path [%s]", path)
		}

		buf := b.Get([]byte(key))
		if buf == nil {
			return errors.Wrapf(ErrKeyNotFound, "key [%s]", key)
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.View(exists)
	} else {
		err = exists(s.tx)
		s.err = err
	}

	if err != nil && !(errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrPathNotFound)) {
		s.store.logger.Debug().Str("err", err.Error()).Msg("KeyExists")
	}

	return err == nil
}

// List keys returns paged collection of keys
func (s *Session) ListKeys(path []string, pageToken string) ([]string, string, error) {
	s.store.logger.Trace().Interface("path", path).Str("pageToken", pageToken).Msg("Session::ListKeys")

	var (
		keys      = make([]string, 0)
		nextToken string
	)

	list := func(tx *bolt.Tx) error {
		b, err := s.setBucket(path)
		if err != nil {
			return err
		}

		cursor := b.Cursor()

		var k []byte
		for i := int32(0); i < pageSize; i++ {
			if i == 0 {
				if pageToken == "" {
					k, _ = cursor.First()
				} else {
					k, _ = cursor.Seek([]byte(pageToken))
				}
			} else {
				k, _ = cursor.Next()
			}
			if k == nil {
				break
			}

			keys = append(keys, string(k))
		}

		k, _ = cursor.Next()
		if k != nil {
			nextToken = string(k)
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.View(list)
	} else {
		err = list(s.tx)
		s.err = err
	}

	if err != nil {
		s.store.logger.Trace().Err(err).Msg("ListKeys")
		return []string{}, "", nil
	}

	return keys, nextToken, nil
}

// PrefixExists scans keys for prefix match
func (s *Session) PrefixExists(path []string, prefix string) (bool, error) {
	s.store.logger.Trace().Interface("path", path).Str("prefix", prefix).Msg("Session::PrefixExists")

	var exists bool

	read := func(tx *bolt.Tx) error {
		b, err := s.setBucket(path)
		if err != nil {
			return errors.Wrapf(ErrPathNotFound, "path [%s]", path)
		}

		c := b.Cursor()

		filter := []byte(prefix)
		for k, _ := c.Seek(filter); k != nil && bytes.HasPrefix(k, filter); k, _ = c.Next() {
			if k == nil {
				break
			}
			if strings.HasPrefix(string(k), prefix) {
				exists = true
				break
			}
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.View(read)
	} else {
		err = read(s.tx)
		s.err = err
	}

	if err != nil {
		s.store.logger.Trace().Err(s.err).Msg("PrefixExists")
		return false, err
	}

	return exists, nil
}

// ReadScan returns list of key-value pairs which match the scan prefix filter.
func (s *Session) ReadScan(path []string, prefix string) ([]string, [][]byte, error) {
	s.store.logger.Trace().Interface("path", path).Str("prefix", prefix).Msg("Session::ReadScan")

	var (
		keys   = make([]string, 0)
		values = make([][]byte, 0)
	)

	read := func(tx *bolt.Tx) error {
		b, err := s.setBucket(path)
		if err != nil {
			return errors.Wrapf(ErrPathNotFound, "path [%s]", path)
		}

		c := b.Cursor()

		prefix := []byte(prefix)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if k == nil {
				break
			}

			fmt.Printf("key=%s, value=%s\n", k, v)
			keys = append(keys, string(k))
			values = append(values, v)
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.View(read)
	} else {
		err = read(s.tx)
		s.err = err
	}

	if err != nil {
		s.store.logger.Trace().Err(s.err).Msg("ReadScan")
		return []string{}, [][]byte{}, err
	}

	return keys, values, nil
}

// Generate next ID for bucket
func (s *Session) NextSeq(path []string) (uint64, error) {
	s.store.logger.Trace().Interface("path", path).Msg("Session::NextID")

	var id uint64

	genID := func(tx *bolt.Tx) error {
		b, err := s.setBucketIfNotExist(path)
		if err != nil {
			return errors.Wrapf(err, "bucket [%s]", path)
		}

		id, err = b.NextSequence()

		return err
	}

	var err error
	if s.tx == nil {
		err = s.store.db.Update(genID)
	} else {
		err = genID(s.tx)
		s.err = err
	}

	return id, err
}

// Write value for key in bucket path.
func (s *Session) Write(path []string, key string, value []byte) error {
	s.store.logger.Trace().Interface("path", path).Str("key", key).Msg("Session::Write")

	write := func(tx *bolt.Tx) error {
		b, err := s.setBucketIfNotExist(path)
		if err != nil {
			return errors.Wrapf(err, "bucket [%s]", path)
		}

		if err := b.Put([]byte(key), value); err != nil {
			return errors.Wrap(err, "createHandler")
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.Update(write)
	} else {
		err = write(s.tx)
		s.err = err
	}

	return err
}

// Delete key, deletes key at given path when present.
// The call does not return an error when key does not exist.
func (s *Session) DeleteKey(path []string, key string) error {
	s.store.logger.Trace().Interface("path", path).Str("key", key).Msg("Session::DeleteKey")

	del := func(tx *bolt.Tx) error {
		b, err := s.setBucketIfNotExist(path)
		if err != nil {
			return nil
		}

		if err := b.Delete([]byte(key)); err != nil {
			return errors.Wrapf(err, "delete path:[%s] key:[%s]", path, key)
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.Update(del)
	} else {
		err = del(s.tx)
		s.err = err
	}

	return err
}

// BucketExists checks if a bucket path exists.
func (s *Session) BucketExists(path []string) bool {
	s.store.logger.Trace().Interface("path", path).Msg("PathExists")

	exists := func(tx *bolt.Tx) error {
		_, err := s.setBucket(path)
		return err
	}

	var err error
	if s.tx == nil {
		err = s.store.db.View(exists)
	} else {
		err = exists(s.tx)
		s.err = err
	}

	if errors.Is(err, ErrPathNotFound) {
		return false
	}

	if err != nil {
		s.store.logger.Debug().Interface("err", err).Msg("PathExists err")
	}

	return err == nil
}

// Create bucket path.
func (s *Session) CreateBucket(path []string) error {
	s.store.logger.Trace().Interface("path", path).Msg("Session::CreateBucket")

	create := func(tx *bolt.Tx) error {
		b, err := s.setBucketIfNotExist(path)
		if err != nil {
			return errors.Wrapf(err, "bucket [%s]", path)
		}
		if b == nil {
			return errors.Wrapf(err, "bucket [%s]", path)
		}
		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.Update(create)
	} else {
		err = create(s.tx)
		s.err = err
	}

	return err
}

// Delete bucket at the tail of the given bucket path.
// The call does not return an error when the bucket does not exist.
func (s *Session) DeleteBucket(path []string) error {
	s.store.logger.Trace().Interface("path", path).Msg("Session::DeleteBucket")

	del := func(tx *bolt.Tx) error {
		if len(path) == 1 {
			return tx.DeleteBucket([]byte(path[0]))
		}

		b, err := s.setBucket(path[:len(path)-1])
		if err != nil {
			return nil
		}
		err = b.DeleteBucket([]byte(path[len(path)-1]))
		if err != nil && errors.Is(err, bolt.ErrBucketNotFound) {
			return nil
		}
		return err
	}

	var err error
	if s.tx == nil {
		err = s.store.db.Update(del)
	} else {
		err = del(s.tx)
		s.err = err
	}

	return err
}

// List buckets, returns a paged collection of buckets.
func (s *Session) ListBuckets(path []string, pageToken string) ([]string, string, error) {
	s.store.logger.Trace().Interface("path", path).Str("pageToken", pageToken).Msg("Session::ListBuckets")

	var (
		buckets   = make([]string, 0)
		nextToken string
	)

	list := func(tx *bolt.Tx) error {

		if len(path) == 0 {
			_ = tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				buckets = append(buckets, string(name))
				return nil
			})
			nextToken = ""
			return nil
		}

		b, err := s.setBucket(path)
		if err != nil {
			return err
		}

		cursor := b.Cursor()

		var k []byte
		for i := int32(0); i < pageSize; i++ {
			if i == 0 {
				if pageToken == "" {
					k, _ = cursor.First()
				} else {
					k, _ = cursor.Seek([]byte(pageToken))
				}
			} else {
				k, _ = cursor.Next()
			}
			if k == nil {
				break
			}

			buckets = append(buckets, string(k))
		}

		k, _ = cursor.Next()
		if k != nil {
			nextToken = string(k)
		}

		return nil
	}

	var err error
	if s.tx == nil {
		err = s.store.db.View(list)
	} else {
		err = list(s.tx)
		s.err = err
	}

	if err != nil {
		s.store.logger.Trace().Err(err).Msg("ListBuckets")
		return []string{}, "", err
	}

	return buckets, nextToken, nil
}

func (s *Session) setBucket(path []string) (*bolt.Bucket, error) {
	var b *bolt.Bucket

	for index, p := range path {
		if index == 0 {
			b = s.tx.Bucket([]byte(p))
		} else {
			b = b.Bucket([]byte(p))
		}
		if b == nil {
			return nil, errors.Wrapf(ErrPathNotFound, "path [%s]", pathStr(path))
		}
	}

	if b == nil {
		return nil, errors.Wrapf(ErrPathNotFound, "path [%s]", pathStr(path))
	}
	return b, nil
}

func (s *Session) setBucketIfNotExist(path []string) (*bolt.Bucket, error) {
	var (
		b   *bolt.Bucket
		err error
	)
	for index, p := range path {
		if index == 0 {
			b, err = s.tx.CreateBucketIfNotExists([]byte(p))
		} else {
			b, err = b.CreateBucketIfNotExists([]byte(p))
		}
		if err != nil {
			return nil, errors.Wrapf(err, "bucket [%s]", p)
		}
	}

	if b == nil {
		return nil, errors.Wrapf(ErrPathNotFound, "path [%s]", pathStr(path))
	}
	return b, nil
}

func pathStr(path []string) string {
	return strings.Join(path, "/")
}
