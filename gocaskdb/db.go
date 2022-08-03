package gocaskdb

import (
	"log"
	"os"
	"time"
)

type DB struct {
	fileName      string
	file          *os.File
	isNew         bool
	writePosition int
	keyDir        map[string]KeyEntry
	encoder       *Encoder
}

type KeyEntry struct {
	timestamp int
	position  int
	totalSize int
}

type KeyValuePair struct {
	timestamp int
	key       string
	value     string
}

const (
	HeaderSize    = 12
	DefaultWhence = 0
)

// InitDB initializes a DB object.
func InitDB(fileName string) *DB {
	db := DB{fileName: fileName, writePosition: 0, keyDir: make(map[string]KeyEntry), encoder: &Encoder{}}
	if _, err := os.Stat(fileName); err == nil {
		db.initKeyDir()
		db.file, err = os.Open(fileName)
		if err != nil {
			log.Fatalln("err opening db file: ", err)
		}
	} else {
		f, err := os.Create(fileName)
		if err != nil {
			log.Fatalln("err creating db file: ", err)
		}
		db.file = f
		db.isNew = true
	}

	return &db
}

// Get retrieves the value from the db and returns it. If the key does not exist it returns an empty string.
func (db *DB) Get(k string) string {
	e, found := db.keyDir[k]
	if !found {
		log.Println("could not find value for key: ", k)
		return ""
	}

	if db.isNew {
		_, err := db.file.Seek(int64(e.position), DefaultWhence)
		if err != nil {
			log.Fatalln("err when setting read offset: ", err)
		}
	}

	b := make([]byte, e.totalSize)
	_, err := db.file.Read(b)
	if err != nil {
		log.Fatalln("read err: ", err)
	}

	kv := db.encoder.DecodeKV(b)
	return kv.value
}

// Set stores the key and value on the disk.
func (db *DB) Set(k string, v string) {
	ts := int(time.Now().Unix())
	size, data := db.encoder.EncodeKV(ts, k, v)
	db.file.Write(data)
	db.file.Sync()
	ke := KeyEntry{timestamp: ts, position: db.writePosition, totalSize: size}
	db.keyDir[k] = ke
	db.writePosition += size
}

func (db *DB) initKeyDir() {
	log.Println("initialising the database ...")

	f, err := os.Open(db.fileName)
	if err != nil {
		log.Fatalln("err opening file: ", err)
	}
	hBytes := make([]byte, HeaderSize)

	for {
		n, _ := f.Read(hBytes)
		if n < HeaderSize {
			break
		}
		if err != nil {
			log.Fatalln("err reading file: ", err)
		}

		ts, keySize, valueSize := db.encoder.DecodeHeader(hBytes)

		keyBytes := make([]byte, keySize)
		_, err = f.Read(keyBytes)
		if err != nil {
			log.Fatalln("err reading key: ", err)
		}
		key := string(keyBytes)

		valBytes := make([]byte, valueSize)
		_, err = f.Read(valBytes)
		if err != nil {
			log.Fatalln("err reading value: ", err)
		}
		val := string(valBytes)

		totalSize := int(HeaderSize + keySize + valueSize)
		kv := KeyEntry{timestamp: int(ts), position: db.writePosition, totalSize: totalSize}
		db.keyDir[key] = kv
		db.writePosition += totalSize

		log.Printf("loaded key: %v, value: %v\n", key, val)

	}

	log.Println("database initialized!")
}

func (db *DB) Close() {
	if err := db.file.Sync(); err != nil {
		log.Println("err syncing file: ", err)
	}

	if err := db.file.Close(); err != nil {
		log.Println("err closing file: ", err)
	}
}
