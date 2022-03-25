package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {
	ctx := context.Background()
	connStr := "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"
	dbpool, err := pgxpool.Connect(ctx, connStr)

	log.Println("starting fresh")
	deleteStuff := `
        DROP TABLE IF EXISTS trades;
        DROP TABLE IF EXISTS symbols;
    `
	_, err = dbpool.Exec(ctx, deleteStuff)
	if err != nil {
		log.Printf("unable to delete stuff: %v\n", err)
	}
	log.Println("dropped table symbols")
	// create symbol table
	queryCreateTable := `
        CREATE TABLE symbols (
            id SERIAL PRIMARY KEY, 
            symbol VARCHAR(50) NOT NULL UNIQUE
        );
        `
	_, err = dbpool.Exec(ctx, queryCreateTable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create symbols table: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Successfully created table symbols")

	queryCreateHypertable := `
        CREATE TABLE trades ( 
            time TIMESTAMPTZ NOT NULL,
            symbol_id INTEGER NOT NULL,
            price DOUBLE PRECISION NOT NULL,
        FOREIGN KEY (
            symbol_id
        )
        REFERENCES symbols (id)
        );
        SELECT create_hypertable('trades', 'time');
    `
	_, err = dbpool.Exec(ctx, queryCreateHypertable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create symbols table: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Successfully created table symbols")
	fmt.Println("Successfully created table trades")
	fmt.Println("Successfully created hypertable")

	// // batch
	// batch := &pgx.Batch{}
	// insertQuery := "INSERT INTO trades (time, symbol_id, price) VALUES ($1, $2, $3);"
	// for i := 0; i < 10; i++ {
	// 	if i%2 == 0 {
	// 		batch.Queue(insertQuery, time.Now(), 1, i)
	// 	} else {
	// 		batch.Queue(insertQuery, time.Now(), 2, i)
	// 	}
	// }
	// br := dbpool.SendBatch(ctx, batch)
	// r := br.QueryRow()
	// log.Println(r)

	// commented out for clean start
	// addFailingTrade := `
	//        insert into "trades" (time, symbol_id, price) values
	//        (CURRENT_TIMESTAMP, (select id from symbols where symbol = 'TSLA'), 12.34);
	//    `
	// _, err = dbpool.Exec(ctx, addFailingTrade)
	// fmt.Println(err)
	// if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23502" {
	//
	// 	addSymbol := `
	//        insert into "symbols" (symbol, name) values ('TSLA', 'Super Rad EV Company');
	//
	//        `
	// 	_, err = dbpool.Exec(ctx, addSymbol)
	// 	if err != nil {
	// 		fmt.Println("could not insert tesla symbol")
	// 	}
	//
	// 	_, err = dbpool.Exec(ctx, addFailingTrade)
	// 	if err != nil {
	// 		fmt.Println("retry failed")
	// 	}
	// }
}
