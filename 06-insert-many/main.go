package main

import (
	"context"
	"fmt"
	"os"
	"time"

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

	//SQL query to generate sample data
	queryDataGeneration := `
       SELECT generate_series(now() - interval '24 hour', now(), interval '5 minute') AS time,
       floor(random() * (3) + 1)::int as sensor_id,
       random()*100 AS temperature,
       random() AS cpu
       `
	//Execute query to generate samples for sensor_data hypertable
	rows, err := dbpool.Query(ctx, queryDataGeneration)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to generate sensor data: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	fmt.Println("Successfully generated sensor data")

	//Store data generated in slice results
	type result struct {
		Time        time.Time
		SensorId    int
		Temperature float64
		CPU         float64
	}
	var results []result
	for rows.Next() {
		var r result
		err = rows.Scan(&r.Time, &r.SensorId, &r.Temperature, &r.CPU)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to scan %v\n", err)
			os.Exit(1)
		}
		results = append(results, r)
	}
	// Any errors encountered by rows.Next or rows.Scan are returned here
	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "rows Error: %v\n", rows.Err())
		os.Exit(1)
	}

	// Check contents of results slice
	fmt.Println("Contents of RESULTS slice")
	for i := range results {
		var r result
		r = results[i]
		fmt.Printf("Time: %s | ID: %d | Temperature: %f | CPU: %f |\n", &r.Time, r.SensorId, r.Temperature, r.CPU)
	}

	//Insert contents of results slice into TimescaleDB
	//SQL query to generate sample data
	queryInsertTimeseriesData := `
   INSERT INTO sensor_data (time, sensor_id, temperature, cpu) VALUES ($1, $2, $3, $4);
   `

	//Insert contents of results slice into TimescaleDB
	for i := range results {
		var r result
		r = results[i]
		_, err := dbpool.Query(ctx, queryInsertTimeseriesData, r.Time, r.SensorId, r.Temperature, r.CPU)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to insert sample into Timescale %v\n", err)
			os.Exit(1)
		}
		defer rows.Close()
	}
	fmt.Println("Successfully inserted samples into sensor_data hypertable")
}
