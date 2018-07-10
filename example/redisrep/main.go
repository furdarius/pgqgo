package main

import (
	"context"
	"database/sql"
	"log"
	"net/url"
	"time"

	"github.com/furdarius/pgqgo"
	"github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

const priceKey = "prices"

// ReplicateRedisProcessor used to replicate data from Postgres to Redis.
type ReplicateRedisProcessor struct {
	pool *redis.Pool
}

// Process inherited from BatchProcessor.
func (p *ReplicateRedisProcessor) Process(ctx context.Context, batchID int, events []pgqgo.Event) ([]pgqgo.RetryEvent, error) {
	log.Printf("batch received: batch_id = %d, size = %d\n", batchID, len(events))

	conn := p.pool.Get()
	defer conn.Close() // nolint:errcheck

	var failed []pgqgo.RetryEvent
	for _, event := range events {
		err := p.sync(ctx, conn, event)
		if err != nil {
			log.Printf("failed to process event %d from batch %d, return it to retry queue: %v", event.ID, event.Data, err)

			failed = append(failed, pgqgo.RetryEvent{
				EventID: event.ID,
				Delay:   10 * time.Second,
			})

			continue
		}

	}

	return failed, nil
}

func (p *ReplicateRedisProcessor) sync(ctx context.Context, conn redis.Conn, event pgqgo.Event) error {
	if !event.Type.Valid || !event.Data.Valid {
		log.Printf("event %d hasn't type or data", event.ID)

		return nil
	}

	op := event.Type.String[0]

	parsed, err := url.Parse("?" + event.Data.String)
	if err != nil {
		return errors.Wrap(err, "failed to parse data string")
	}

	data := parsed.Query()

	itemID := data.Get("item_id")
	if itemID == "" {
		return errors.New("item_id not found in event data")
	}

	price := data.Get("price")
	if price == "" {
		return errors.New("price not found in event data")
	}

	log.Printf("event received: op=%s item_id=%s price=%s", string(op), itemID, price)

	if op == 'I' || op == 'U' {
		_, err = conn.Do("HSET", priceKey, itemID, price)
		if err != nil {
			return errors.Wrap(err, "failed to set data to redis")
		}
	} else if op == 'D' {
		_, err = conn.Do("HDEL", priceKey, itemID)
		if err != nil {
			return errors.Wrap(err, "failed to delete data from redis")
		}
	}

	return nil
}

func newRedisPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

func main() {
	pgdsn := "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
	redisPool := newRedisPool("127.0.0.1:6379")

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

	processor := &ReplicateRedisProcessor{redisPool}
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
