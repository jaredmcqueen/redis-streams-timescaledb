package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {
	ctx := context.Background()
	connStr := "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"
	dbpool, err := pgxpool.Connect(ctx, connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	//Create relational table called sensors
	queryCreateTable := `CREATE TABLE sensors (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50));`
	_, err = dbpool.Exec(ctx, queryCreateTable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create SENSORS table: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Successfully created relational table SENSORS")

}
