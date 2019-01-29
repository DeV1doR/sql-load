package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	log "github.com/sirupsen/logrus"
)

var (
	dbURL         string
	consumerCount uint
	threadCount   uint
	duration      uint
)

type TransactionLog struct {
	gorm.Model
	CallbackID string    `gorm:"size:255`
	Date       time.Time `gorm:"default:CURRENT_TIMESTAMP"`
	Reference  string    `gorm:"size:255`
	Type       string    `gorm:"size:16`
	Ref        string    `gorm:"size:16`
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func getAverageFloat64(sl []float64) float64 {
	var total float64 = 0
	for _, value := range sl {
		total += value
	}
	return total / float64(len(sl)*1000000)
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Info(fmt.Sprintf("INSERT TIME: %f, fund: %s", elapsed.Seconds(), name))
}

func load(db *gorm.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	doc := &TransactionLog{
		CallbackID: "qwerty12345",
		Reference:  "10101",
		Type:       "go",
	}
	defer timeTrack(time.Now(), "load")
	if dbc := db.Create(doc); dbc.Error != nil {
		log.Info(dbc.Error)
		return
	}
}

func init() {
	flag.StringVar(&dbURL, "url", "postgresql://postgres:postgres@localhost:5432?database=postgres&sslmode=disable", "DB url")
	flag.UintVar(&consumerCount, "consumers", 1, "a number of consumers")
	flag.UintVar(&threadCount, "threads", 1, "a number of threads")
	flag.UintVar(&duration, "duration", 1, "duration for execution")

	runtime.GOMAXPROCS(int(threadCount))

	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)

	rand.Seed(time.Now().UnixNano())
}

func main() {
	flag.Parse()

	db, err := gorm.Open("postgres", dbURL)
	if err != nil {
		panic(err)
	}
	db.DB().SetMaxIdleConns(100)
	db.DB().SetMaxOpenConns(1500)
	db.AutoMigrate(&TransactionLog{})

	now := time.Duration(time.Now().Unix())
	endTime := now + time.Duration(duration)

	log.WithFields(log.Fields{
		"now":       now,
		"end-at":    endTime,
		"consumers": consumerCount,
		"threads":   threadCount,
		"duration":  duration,
		"db-url":    dbURL,
	}).Info("Start loading...")

	var wg sync.WaitGroup

	for time.Duration(time.Now().Unix()) < endTime {
		log.Debug("Added new waiters")
		wg.Add(int(consumerCount))
		for i := 0; i < int(consumerCount); i++ {
			log.Debug(fmt.Sprintf("Goroutine #%+v started", RandStringRunes(16)))
			go load(db, &wg)
		}
		log.Debug("Goroutines finished")
	}
	wg.Wait()
	log.WithFields(log.Fields{
		"duration": duration,
	}).Info("Load finished")
	defer db.Close()
}
