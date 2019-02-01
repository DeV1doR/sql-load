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
	dbURL              string
	consumerCount      int
	threadCount        int
	duration           int
	openedConnections  int
	idleConnections    int
	connectionLifetime int
	countAll           int
	countSuccess       int
	tmp                map[string][]float64
	m                  sync.RWMutex
)

type TransactionLog struct {
	ID         uint `gorm:"primary_key"`
	User       User `gorm:"foreignkey:UserID"`
	UserID     uint
	CallbackID string    `gorm:"size:255"`
	Date       time.Time `gorm:"default:CURRENT_TIMESTAMP;index"`
	Reference  string    `gorm:"size:255"`
	Type       string    `gorm:"size:16"`
	Ref        string    `gorm:"size:16"`
	Data       string    `gorm:"size:255"`
	Comment    string    `gorm:"size:255"`
	Tenant     string    `gorm:"size:255"`
	Amount     float64   `gorm:"index";sql:"type:decimal(10,2);"`
}

type User struct {
	ID       uint    `gorm:"primary_key"`
	Email    string  `gorm:"size:255;NOT NULL"`
	Nickname string  `gorm:"size:64;NOT NULL;index"`
	Amount   float64 `sql:"type:decimal(10,2);"`
}

func CreateHardcodedTransactionLogWithUser(db *gorm.DB, user *User) error {
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err := tx.Error; err != nil {
		return err
	}
	transaction := &TransactionLog{
		CallbackID: "qwerty12345",
		Reference:  "10101",
		Type:       "go",
		Ref:        "heloshka",
		Data:       "some meta info",
		Comment:    "some comment info",
		Tenant:     "tur za tushur",
		UserID:     user.ID,
		Amount:     1,
	}
	start := time.Now()
	{
		if err := tx.Create(transaction).Error; err != nil {
			tx.Rollback()
			return err
		}
	}
	elapsed := time.Since(start).Seconds()
	log.Debug(fmt.Sprintf("tx.Create SECS: %.6f", elapsed))

	m.Lock()
	{
		user.Amount += transaction.Amount
		tmp["Create"] = append(tmp["Create"], elapsed)
	}
	m.Unlock()

	start = time.Now()
	{
		if err := tx.Save(user).Error; err != nil {
			tx.Rollback()
			return err
		}
	}
	elapsed = time.Since(start).Seconds()
	log.Debug(fmt.Sprintf("tx.Save SECS: %.6f", elapsed))

	m.Lock()
	{
		tmp["Save"] = append(tmp["Save"], elapsed)
	}
	m.Unlock()

	return tx.Commit().Error
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
	return total / float64(len(sl))
}

func init() {
	flag.StringVar(&dbURL, "url", "postgresql://postgres:postgres@localhost:5432?database=postgres&sslmode=disable&connect_timeout=5", "DB url")
	flag.IntVar(&consumerCount, "consumers", 1, "a number of consumers")
	flag.IntVar(&threadCount, "threads", 1, "a number of threads")
	flag.IntVar(&duration, "duration", 1, "duration for execution")
	flag.IntVar(&openedConnections, "connections", 1, "a number of connection pool to DB")
	flag.IntVar(&idleConnections, "idle", 1, "a number of idle connection to DB")
	flag.IntVar(&connectionLifetime, "lifetime", 0, "lifetime of connection")

	runtime.GOMAXPROCS(int(threadCount))

	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)

	rand.Seed(time.Now().UnixNano())

	tmp = make(map[string][]float64)
}

func main() {
	flag.Parse()

	db, err := gorm.Open("postgres", dbURL)
	if err != nil {
		panic(err)
	}
	db.AutoMigrate(&User{})
	db.AutoMigrate(&TransactionLog{})

	db.DB().SetMaxOpenConns(openedConnections)
	db.DB().SetMaxIdleConns(idleConnections)
	db.DB().SetConnMaxLifetime(time.Duration(connectionLifetime))

	now := time.Duration(time.Now().Unix())
	endTime := now + time.Duration(duration)

	ch := make(chan bool)

	log.WithFields(log.Fields{
		"now":       now,
		"end-at":    endTime,
		"consumers": consumerCount,
		"threads":   threadCount,
		"duration":  duration,
		"db-url":    dbURL,
	}).Info("Start loading...")

	tps := time.Duration(1000000000/consumerCount) * time.Nanosecond
	td := time.After(time.Duration(duration) * time.Second)

	ticker := time.Tick(tps)

	var user User
	db.FirstOrCreate(&user, User{
		Email:    "test@example.com",
		Nickname: "youruniquenickname",
	})
	go func() {
		for {
			select {
			case <-ticker:
				go func() {
					start := time.Now()
					if err := CreateHardcodedTransactionLogWithUser(db, &user); err != nil {
						ch <- false
					} else {
						elapsed := time.Since(start).Seconds()
						log.Debug(fmt.Sprintf("tx.Commit SECS: %.6f", elapsed))
						tmp["Commit"] = append(tmp["Commit"], elapsed)
						ch <- true
					}
				}()
				countAll++
			}
		}
	}()
	func() {
		for {
			select {
			case result := <-ch:
				if result {
					countSuccess++
				}
				log.Debug(fmt.Sprintf("Goro exec succsess: %v", result))
			case <-td:
				return
			}
		}
	}()
	log.WithFields(log.Fields{
		"count":   countAll,
		"success": countSuccess,
		"mean": map[string]string{
			"Create": fmt.Sprintf("%.6f", getAverageFloat64(tmp["Create"])),
			"Save":   fmt.Sprintf("%.6f", getAverageFloat64(tmp["Save"])),
			"Commit": fmt.Sprintf("%.6f", getAverageFloat64(tmp["Commit"])),
		},
	}).Info("Load finished")
	defer db.Close()
}
