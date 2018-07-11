<p align="center"><img src="https://habrastorage.org/webt/l6/h6/to/l6h6tofwsw4s-rjd6iahhjzldr0.png"></p>

[![GoDoc](https://godoc.org/github.com/furdarius/pgqgo?status.svg)](https://godoc.org/github.com/furdarius/pgqgo)
[![Build Status](https://travis-ci.org/furdarius/pgqgo.svg?branch=master)](https://travis-ci.org/furdarius/pgqgo)
[![Go Report Card](https://goreportcard.com/badge/github.com/furdarius/pgqgo)](https://goreportcard.com/report/github.com/furdarius/pgqgo)

# PGQ consuming from Go

Small, user-friendly library for consuming PGQ.

## Install
```
go get github.com/furdarius/pgqgo
```

### Adding as dependency by "go dep"
```
$ dep ensure -add github.com/furdarius/pgqgo
```

## Usage

* Implement [Processor](https://godoc.org/github.com/furdarius/pgqgo#BatchProcessor)
* [Register Consumer](https://godoc.org/github.com/furdarius/pgqgo#Consumer.Register)
* [Start Consuming](https://godoc.org/github.com/furdarius/pgqgo#Consumer.Start)


Usage example:

```go
// LogProcessor used to output received batch id, it's size and data.
// Implements pgqgo.BatchProcessor.
type LogProcessor struct{}

// Process consequentially output to stdout each events data from batch.
func (p *LogProcessor) Process(ctx context.Context, batchID int, events []pgqgo.Event) ([]pgqgo.RetryEvent, error) {
	log.Printf("batch received: batch_id = %d, size = %d\n", batchID, len(events))

	for _, event := range events {
		fmt.Println(event)
	}

	return nil, nil
}

func main() {
	db, _ := sql.Open("postgres", "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable")

	// Create consumer with batch processor.
	processor := &LogProcessor{}
	consumer := pgqgo.NewConsumer(db, processor, "consumer_name")

	queue := "queue_name"
	
	err = consumer.Register(ctx, queue)
	if err != nil && err != pgqgo.ErrAlreadyExists {
		log.Fatalf("failed to register pgq consumer: %v", err)
	}

	err = consumer.Start(ctx, queue)
	if err != nil && err != context.Canceled {
		log.Fatalf("failed to start pgq consumer: %v", err)
	}
}
```

[More examples here](https://github.com/furdarius/pgqgo/example)
