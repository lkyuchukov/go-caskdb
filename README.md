# go-caskdb
A KV store in Go built as a learning exercise.
Supports high throughput, low latency for reads and writes and it is easy to back up/restore. 

## Usage

```Go
db := gocaskdb.InitDB("awesome.db")
db.Set("foo", "bar")
db.Set("boo", "bam")

v := db.Get("foo") // bar
v2 := db.Get("boo") // bam

db.Close()
```
