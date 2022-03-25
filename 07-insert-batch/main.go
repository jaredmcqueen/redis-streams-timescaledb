package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	connStr := "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"
	dbpool, err := pgxpool.Connect(ctx, connStr)
	batch := &pgx.Batch{}
	batch.Queue("select count(*) from sensor_data")
	br := dbpool.SendBatch(ctx, batch)
	_, err = br.Exec()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to execute statement in batch queue %v\n", err)
		os.Exit(1)
	}

	var ra int
	err = br.QueryRow().Scan(&ra)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(ra)
}
