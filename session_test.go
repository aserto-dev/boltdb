package boltdb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListBucketsEmpty(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.ReadSession()
	require.NoError(t, err)
	t.Cleanup(closer)

	buckets, nextToken, err := session.ListBuckets([]string{}, "")
	assert.Nil(t, err)
	assert.Empty(t, nextToken)
	assert.Equal(t, len(buckets), 0)

	t.Logf("buckets %q", buckets)
}

func TestListBucketsRoots(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.WriteSession()
	require.NoError(t, err)

	err = session.Write([]string{"l1a", "l2a", "l3a"}, "k1a", []byte("v1a"))
	assert.NoError(t, err)
	err = session.Write([]string{"l1a", "l2a", "l3a"}, "k2a", []byte("v2a"))
	assert.NoError(t, err)
	err = session.Write([]string{"l1a", "l2a", "l3a"}, "k3a", []byte("v3a"))
	assert.NoError(t, err)

	if b, errX := session.Read([]string{"l1a", "l2a", "l3a"}, "k1a"); errX == nil {
		t.Logf("read v1a %s", string(b))
	}

	err = session.Write([]string{"l1b", "l2b", "l3b"}, "k1b", []byte("v1b"))
	assert.NoError(t, err)

	if b, errX := session.Read([]string{"l1b", "l2b", "l3b"}, "k1b"); errX == nil {
		t.Logf("read v1b %s", string(b))
	}

	buckets, nextToken, err := session.ListBuckets([]string{}, "")
	assert.Nil(t, err)
	assert.Empty(t, nextToken)
	assert.Equal(t, len(buckets), 2)

	t.Logf("buckets %q", buckets)
	closer()
}

func TestListBucketsSubLevel(t *testing.T) {
	var err error

	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.WriteSession()
	require.NoError(t, err)

	err = session.Write([]string{"l1a", "l2a", "l3a"}, "k1a", []byte("v1a"))
	assert.NoError(t, err)
	err = session.Write([]string{"l1a", "l2b", "l3a"}, "k2a", []byte("v2a"))
	assert.NoError(t, err)
	err = session.Write([]string{"l1a", "l2c", "l3a"}, "k3a", []byte("v3a"))
	assert.NoError(t, err)

	if b, errX := session.Read([]string{"l1a", "l2a", "l3a"}, "k1a"); errX == nil {
		if string(b) != "v1a" {
			t.Errorf("read fail")
			t.FailNow()
		}
		t.Logf("read v1a %s", string(b))
	}

	err = session.Write([]string{"l1a", "l2a", "l3b"}, "k1b", []byte("v1b"))
	assert.NoError(t, err)
	if b, err := session.Read([]string{"l11", "l2a", "l3b"}, "k1b"); err == nil {
		t.Logf("read v1b %s", string(b))
	}

	{
		buckets, nextToken, err := session.ListBuckets([]string{"l1a"}, "")
		assert.NoError(t, err)
		assert.Empty(t, nextToken)
		assert.Len(t, buckets, 3)

		t.Logf("buckets %q", buckets)
	}

	{
		buckets, nextToken, err := session.ListBuckets([]string{"l1a", "l2a"}, "")
		assert.NoError(t, err)
		assert.Empty(t, nextToken)
		assert.Len(t, buckets, 2)

		t.Logf("buckets %q", buckets)
	}

	closer()
}

func TestListKeys(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.ReadSession()
	require.NoError(t, err)

	if keys, next, err := session.ListKeys([]string{"l1a", "l2a", "l3a"}, ""); err == nil {
		t.Logf("keys %q", keys)
		t.Logf("next %s", next)
		t.Logf("total %d", len(keys))
	}

	closer()
}

func TestListValues(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.ReadSession()
	require.NoError(t, err)

	if keys, values, next, err := session.List([]string{"l1a", "l2a", "l3a"}, ""); err == nil {
		t.Logf("keys %q", keys)
		t.Logf("values %q", values)
		t.Logf("next %s", next)
		t.Logf("total %d", len(keys))
	}

	closer()
}

func TestBucketExists(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.ReadSession()
	require.NoError(t, err)

	if session.BucketExists([]string{"l1a"}) != true {
		t.FailNow()
	}
	if session.BucketExists([]string{"l1a", "l2a", "l3a"}) != true {
		t.FailNow()
	}
	if session.BucketExists([]string{"l1a", "l2a", "k3a"}) == true {
		t.FailNow()
	}

	closer()
}

func TestKeyExists(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.ReadSession()
	require.NoError(t, err)

	if session.KeyExists([]string{"l1a", "l2a", "l3a"}, "k1a") != true {
		t.FailNow()
	}
	if session.KeyExists([]string{"l1a", "l2a", "l3a"}, "") == true {
		t.FailNow()
	}
	if session.KeyExists([]string{"l1a", "l2a", "l3a"}, "K2A") == true {
		t.FailNow()
	}

	closer()
}

func TestDeleteValue(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.WriteSession()
	require.NoError(t, err)

	if err := session.DeleteKey([]string{"l1a", "l2a", "l3a"}, "k2a"); err != nil {
		t.Logf("Delete %v", err)
		t.FailNow()
	}
	if session.KeyExists([]string{"l1a", "l2a", "l3a"}, "k2a") == true {
		t.FailNow()
	}

	closer()
}

func TestDeleteBucket(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.WriteSession()
	require.NoError(t, err)

	if err := session.DeleteBucket([]string{"l1a", "l2a", "l3a"}); err != nil {
		t.Logf("Delete %v", err)
		t.FailNow()
	}
	if session.BucketExists([]string{"l1a", "l2a", "l3a"}) == true {
		t.FailNow()
	}

	closer()
}

func TestReadSession(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	{
		session, closer, err := s.WriteSession()
		require.NoError(t, err)

		if err := session.Write([]string{"test"}, "read-key", []byte("read-value")); err != nil {
			assert.NoError(t, err)
		}

		closer()
	}
	{
		session, closer, err := s.ReadSession()
		require.NoError(t, err)

		buf, err := session.Read([]string{"test"}, "read-key")
		assert.NoError(t, err)
		str := string(buf)
		assert.Equal(t, str, "read-value")

		closer()
	}
}

func TestWriteSessions(t *testing.T) {
	s := setupStore(t)
	t.Cleanup(s.Close)

	session, closer, err := s.WriteSession()
	require.NoError(t, err)
	t.Cleanup(closer)

	if err := session.Write([]string{"test"}, "key", []byte("hello")); err != nil {
		assert.NoError(t, err)
	}

	buf, err := session.Read([]string{"test"}, "key")
	assert.NoError(t, err)
	str := string(buf)
	assert.Equal(t, str, "hello")
}
