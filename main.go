package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())
var verbose bool
var db *sqlx.DB

func initTable() {
	log.Println("starting table initialize")
	db.MustExec("DROP TABLE IF EXISTS testdata")
	db.MustExec("CREATE TABLE testdata (id varchar(36), data text, PRIMARY KEY (id))")
	log.Println("table initialized")
}

func main() {
	var records int64
	var recordLength int
	var doInit bool

	flag.BoolVar(&verbose, "verbose", false, "enable verbose output")
	flag.BoolVar(&doInit, "init", false, "drop and create test table")
	flag.Int64Var(&records, "records", 1000, "number of records generate")
	flag.IntVar(&recordLength, "length", 512, "record length")
	flag.Parse()

	var err error

	dsn := os.Getenv("DSN")
	if dsn == "" {
		log.Fatal("DSN environment variable required. user=username password=string host=127.0.0.1 port=5432 dbname=postgres sslmode=disable")
	}

	db, err = sqlx.Connect("postgres", os.Getenv("DSN"))
	if err != nil {
		log.Fatal(err)
	}

	if doInit {
		initTable()
	}

	var i int64
	var tx *sqlx.Tx
	for i = 0; i < records; i++ {
		if i%1000 == 0 {
			log.Printf("BEGIN (%d)\n", i)
			tx, err = db.Beginx()
			if err != nil {
				log.Fatal(err)
			}
		}
		_, err = tx.Exec("INSERT INTO testdata (id, data) VALUES ($1, $2)",
			uuid.NewString(), randStringBytes(recordLength))
		if err != nil {
			log.Println(err)
			tx.Rollback()
			return
		}
		if i%1000 == 999 || i == records-1 {
			log.Printf("COMMIT (%d)\n", i)
			err = tx.Commit()
			if err != nil {
				log.Println(err)
				tx.Rollback()
				return
			}
		}
	}
}

func randStringBytes(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)

	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMax); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return sb.String()
}
