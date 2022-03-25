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

	// Slices of sample data to insert
	// observation i has type sensorTypes[i] and location sensorLocations[i]
	sensorTypes := []string{"a", "a", "b", "b"}
	sensorLocations := []string{"floor", "ceiling", "floor", "ceiling"}

	for i := range sensorTypes {
		//INSERT statement in SQL
		queryInsertMetadata := `INSERT INTO sensors (type, location) VALUES ($1, $2);`

		//Execute INSERT command
		_, err := dbpool.Exec(ctx, queryInsertMetadata, sensorTypes[i], sensorLocations[i])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to insert data into database: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Inserted sensor (%s, %s) into database \n", sensorTypes[i], sensorLocations[i])

	}
	fmt.Println("Successfully inserted all sensors into database")

}
