package main

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	// test redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis", err)
	}

	r, _ := rdb.XInfoConsumers(ctx, "trades", "mygroup").Result()
	log.Println(r)
	// r = rdb.XInfoGroups(ctx, "trades")
	// log.Println(r)
	// r, _ = rdb.XInfoStream(ctx, "trades").Result()
	// log.Println(r)

	// consumer group
	err = rdb.XGroupCreate(ctx, "trades", "mygroup", "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Println("error", err)
	}

	err = rdb.XGroupCreateConsumer(ctx, "trades", "mygroup", "consumer1").Err()
	if err != nil {
		log.Println("error creating consumer", err)
	}

	for {
		// time how long the redis grab takes
		trades, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "mygroup",
			Consumer: "consumer1",
			Streams:  []string{"trades", "0"},
			Count:    10,
			Block:    0, // will block forever until events arrive
			NoAck:    false,
		}).Result()
		if err != nil {
			log.Println("error xreadgroup", err)
		}

		// time how long the insert to TS takes
		for _, stream := range trades {
			for _, message := range stream.Messages {
				// log.Println(message)
				S := message.Values["S"].(string)
				t := message.Values["t"].(string)
				p := message.Values["p"].(string)
				s := message.Values["s"].(string)

				log.Println(S, t, p, s)
			}
		}
		time.Sleep(3 * time.Second)

	}

}
