package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/furdarius/pgqgo"
	_ "github.com/lib/pq"
)

// LogProcessor used to output received batch id, it's size and data.
type LogProcessor struct{}

// Process inherited from BatchProcessor.
func (p *LogProcessor) Process(ctx context.Context, batchID int, events []pgqgo.Event) ([]pgqgo.RetryEvent, error) {
	log.Printf("batch received: batch_id = %d, size = %d\n", batchID, len(events))

	for _, event := range events {
		fmt.Println(event)
	}

	return nil, nil
}

func main() {
	pgdsn := "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"

	db, err := sql.Open("postgres", pgdsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close() // nolint:errcheck

	ctx := context.Background()

	err = db.PingContext(ctx)
	if err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	log.Println("database connection successfully established")

	processor := &LogProcessor{}
	consumer := pgqgo.NewConsumer(db, processor, "prices_consumer")

	queue := "prices_q"

	err = consumer.Register(ctx, queue)
	if err != nil && err != pgqgo.ErrAlreadyExists {
		log.Fatalf("failed to register pgq consumer: %v", err)
	}

	err = consumer.Start(ctx, queue)
	if err != nil && err != context.Canceled {
		log.Fatalf("failed to start pgq consumer: %v", err)
	}
}
