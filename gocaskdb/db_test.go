package gocaskdb_test

import (
	"go-caskdb/gocaskdb"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAndSet(t *testing.T) {
	os.CreateTemp("gocaskdb", "temp.db")

	db := gocaskdb.InitDB("temp.db")

	db.Set("foo", "bar")
	db.Set("boo", "bam")

	v := db.Get("foo")
	v2 := db.Get("boo")

	assert.Equal(t, "bar", v)
	assert.Equal(t, "bam", v2)

	db.Close()
}
