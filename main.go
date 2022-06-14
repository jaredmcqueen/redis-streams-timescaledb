package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaredmcqueen/redis-streams-timescaledb/util"
)

var tsdbCounter int32

func init() {
	rand.Seed(time.Now().UnixNano())
}

func redisConsumer(batchChan chan<- []map[string]interface{}, config util.Config) {
	rctx := context.Background()
	log.Println("connecting to redis endpoint", config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: config.RedisEndpoint,
	})

	// test redis connection
	_, err := rdb.Ping(rctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis", err)
	}
	log.Println("connected to redis")

	pit := config.RedisStreamStart
	for {
		trades, err := rdb.XRead(rctx,
			&redis.XReadArgs{
				Streams: []string{"trades", pit},
				Count:   config.TimescaleDBBatchSize,
				Block:   0,
			},
		).Result()
		if err != nil {
			log.Println("error XRead: ", err)
			log.Println("attempting again in 10 seconds")
			time.Sleep(time.Second * 10)
			continue
		}

		bigBatch := make([]map[string]interface{}, 0, config.TimescaleDBBatchSize)

		for _, stream := range trades {
			for _, message := range stream.Messages {
				bigBatch = append(bigBatch, message.Values)
				pit = message.ID
			}
		}
		batchChan <- bigBatch
	}
}

func timescaleWriter(batchChan <-chan []map[string]interface{}, config util.Config) {
	pctx := context.Background()
	dbpool, _ := pgxpool.Connect(pctx, config.TimescaleDBConnection)

	insertTradeSQL := `
        INSERT INTO "trades" (time, symbol, price, tradeSize, tradeCondition, exchangeCode, tape) values
        ($1, $2, $3, $4, $5, $6, $7);
    `

	//block forever
	for {
		select {
		case batch := <-batchChan:
			priBatch := &pgx.Batch{}
			for _, v := range batch {

				tsdbCounter++
				dateMilli, _ := strconv.ParseInt(fmt.Sprintf("%s", v["t"]), 10, 64)
				unixTime := time.UnixMilli(dateMilli).Format(time.RFC3339Nano)
				priBatch.Queue(
					insertTradeSQL,
					unixTime,
					v["S"],                             // symbol
					v["p"],                             // price
					v["s"],                             // tradeSize
					strings.Split(v["c"].(string), ""), // conditions
					v["x"],                             // exchangeCode
					v["z"],                             // tape
				)
			}
			err := dbpool.SendBatch(pctx, priBatch).Close()
			if err != nil {
				log.Fatal("error sending batch ", err)
			}
		}
	}
}

func main() {
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal("could not load config", err)
	}
	log.Printf("%+v", config)
	batchChan := make(chan []map[string]interface{}, 100)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	// Ensure that the tables exist
	pctx := context.Background()
	dbpool, _ := pgxpool.Connect(pctx, config.TimescaleDBConnection)

	sqlCreateTradesTable := `
        CREATE TABLE trades ( 
            time TIMESTAMPTZ NOT NULL,
            symbol VARCHAR,
            price DOUBLE PRECISION,
            tradeSize int NOT NULL,
            tradeCondition VARCHAR ARRAY,
            exchangeCode VARCHAR,
            tape VARCHAR
        );
        SELECT create_hypertable('trades', 'time');
    `
	log.Println("making sure trades table exists")
	_, err = dbpool.Exec(pctx, sqlCreateTradesTable)

	if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code != "42P07" {
		// if pgErr, ok := err.(*pgconn.PgError); ok {
		// log.Println(pgErr)
		log.Fatal("something went wrong creating trades table ", err)
	}

	log.Println("done creating table")
	go redisConsumer(batchChan, config)

	for i := 0; i < config.TimescaleDBWorkers; i++ {
		go timescaleWriter(batchChan, config)
	}

	go func() {
		for {
			log.Println("events per second", tsdbCounter, "cache", len(batchChan))
			tsdbCounter = 0
			time.Sleep(time.Second)
		}
	}()

	<-signalChan
	log.Println("exiting app")
}
