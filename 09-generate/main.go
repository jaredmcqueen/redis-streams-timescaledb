package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var symbolCount = 10_000
var batchSize = 10_000
var ctx = context.Background()
var connStr = "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"
var dbpool, _ = pgxpool.Connect(ctx, connStr)

const (
	alphabet = "abcdefghijklmnopqrstuvwxyz"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func RandomPrice(oldPrice float64) float64 {
	volatility := float64(0.02)
	changePercent := 2 * volatility * rand.Float64()
	if changePercent > volatility {
		changePercent -= (2 * volatility)
	}
	changeAmount := oldPrice * changePercent
	return oldPrice + changeAmount
}

func RandomSymbol() string {
	var sb strings.Builder
	k := len(alphabet)
	for i := 0; i < 3; i++ {
		c := alphabet[rand.Intn(k)]
		sb.WriteByte(c)
	}
	return sb.String()
}

type trade struct {
	symbol string
	price  float64
}

func batchWriter(trades []trade) {
	log.Println("writing a batch of", len(trades))
	insertTradeSQL := `
	       insert into "trades" (time, symbol_id, price) values
	       (CURRENT_TIMESTAMP, (select id from symbols where symbol = $1), $2);
	   `
	addSymbol := `
	   insert into "symbols" (symbol) values ($1);
	   `
	priBatch := &pgx.Batch{}
	for _, t := range trades {
		priBatch.Queue(insertTradeSQL, t.symbol, t.price)
	}

	br := dbpool.SendBatch(ctx, priBatch)

	resends := []trade{}
	// check the primary batch for any errors
	for i := 0; i < priBatch.Len(); i++ {
		_, err := br.Exec()
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23502" {
			if err != nil {
				resends = append(resends, trades[i])
			}
			_, err = dbpool.Exec(ctx, addSymbol, trades[i].symbol)
			if err != nil {
				log.Println("adding a symbol failed", err)
			}
		}
	}
	br.Close()

	if len(resends) > 0 {
		log.Println("we need to resend", len(resends))
		priBatch := &pgx.Batch{}
		for _, t := range trades {
			priBatch.Queue(insertTradeSQL, t.symbol, t.price)
		}

		br := dbpool.SendBatch(ctx, priBatch)

		// check the primary batch for any errors
		for i := 0; i < priBatch.Len(); i++ {
			_, err := br.Exec()
			fmt.Println("there should be no error", err)
		}
		br.Close()
	}
}

func tradeConsumer(batchChan <-chan trade) {
	log.Println("starting tradeConsumer")
	var trades []trade
	for {
		select {
		case t := <-batchChan:
			trades = append(trades, t)
			if len(trades) >= batchSize {
				start := time.Now()
				batchWriter(trades)
				log.Println("processed batch in", time.Since(start))
				trades = []trade{}
			}
		}
	}
}

func tradeProducer(batchChan chan<- trade) {
	log.Println("starting tradeProducer")
	symbolSet := make(map[string]float64)
	for len(symbolSet) < symbolCount {
		symbolSet[RandomSymbol()] = float64(100)
	}
	var printCounter int32
	for {
		for k, v := range symbolSet {
			printCounter++
			// log.Println(printCounter)
			batchChan <- trade{symbol: k, price: float64(printCounter)}
			// time.Sleep(100 * time.Millisecond)
			symbolSet[k] = RandomPrice(v)
		}
	}
}

func main() {
	batchChan := make(chan trade, 100)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go tradeProducer(batchChan)

	for i := 0; i < 4; i++ {
		go tradeConsumer(batchChan)
	}
	go func() {
		for {
			log.Println("queue size", len(batchChan))
			time.Sleep(time.Second)
		}
	}()

	log.Println("waiting for control+c")
	<-signalChan
}
