package main

import (
	"fmt"
	"go-caskdb/gocaskdb"
)

func main() {
	db := gocaskdb.InitDB("foo.db")

	db.Set("foo", "bar")
	db.Set("boo", "bam")
	db.Set("voo", "vam")
	db.Set("moo", "mam")

	v := db.Get("foo")
	v2 := db.Get("boo")
	v3 := db.Get("voo")
	v4 := db.Get("moo")

	fmt.Println(v)
	fmt.Println(v2)
	fmt.Println(v3)
	fmt.Println(v4)

	db.Close()
}
