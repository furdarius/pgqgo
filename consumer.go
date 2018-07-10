package pgqgo

import (
	"context"
	"database/sql"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrAlreadyExists used to notify that entity already exists.
	ErrAlreadyExists = errors.New("already exists")

	// errBatchNotFound used to notify that batch was not found.
	errBatchNotFound = errors.New("batch not found")
)

// Events table structure
// +-----------+--------------------------+----------------------------------+
// | Column    | Type                     | Modifiers                        |
// |-----------+--------------------------+----------------------------------|
// | ev_id     | bigint                   |  not null                        |
// | ev_time   | timestamp with time zone |  not null                        |
// | ev_txid   | bigint                   |  not null default txid_current() |
// | ev_owner  | integer                  |                                  |
// | ev_retry  | integer                  |                                  |
// | ev_type   | text                     |                                  |
// | ev_data   | text                     |                                  |
// | ev_extra1 | text                     |                                  |
// | ev_extra2 | text                     |                                  |
// | ev_extra3 | text                     |                                  |
// | ev_extra4 | text                     |                                  |
// +-----------+--------------------------+----------------------------------+

// Event stores event information.
type Event struct {
	ID     int64
	Time   time.Time
	TxID   int64
	Retry  sql.NullInt64
	Type   sql.NullString
	Data   sql.NullString
	Extra1 sql.NullString
	Extra2 sql.NullString
	Extra3 sql.NullString
	Extra4 sql.NullString
}

// RetryEvent stores information when Event with defined ID must be retried.
type RetryEvent struct {
	EventID int64
	Delay   time.Duration
}

// BatchProcessor provides events batch processing functionality.
type BatchProcessor interface {
	Process(ctx context.Context, batchID int, events []Event) ([]RetryEvent, error)
}

// Consumer used to work with PGQ consuming utilities.
type Consumer struct {
	db        *sql.DB
	processor BatchProcessor
	name      string
	wait      time.Duration
}

// NewConsumer returns Consumer.
func NewConsumer(db *sql.DB, processor BatchProcessor, name string) *Consumer {
	// TODO: Configuration via options

	return &Consumer{
		db:        db,
		processor: processor,
		name:      name,
		wait:      2 * time.Second,
	}
}

// Register used to register consumer for queue.
// @see https://github.com/markokr/skytools/blob/master/sql/pgq/functions/pgq.register_consumer.sql
func (c *Consumer) Register(ctx context.Context, queue string) error {
	const q = `select pgq.register_consumer($1, $2) as st;`

	// st shows state of operations:
	// 0  - if already registered
	// 1  - if new registration
	st, err := c.fetchNumber(ctx, q, queue, c.name)
	if err != nil {
		return err
	}

	if st == 0 {
		return ErrAlreadyExists
	}

	return nil
}

// Unregister used to unregister consumer from queue.
// @see https://github.com/markokr/skytools/blob/master/sql/pgq/functions/pgq.unregister_consumer.sql
func (c *Consumer) Unregister(ctx context.Context, queue string) error {
	const q = `select pgq.unregister_consumer($1, $2) as cnt;`

	_, err := c.fetchNumber(ctx, q, queue, c.name)

	return err
}

// Start used to start consuming queue.
// On not empty events batch BatchProcessor.Process will be called.
func (c *Consumer) Start(ctx context.Context, queue string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := c.processNextBatch(ctx, queue)

		if err == errBatchNotFound {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.wait):
				continue
			}
		}

		if err != nil {
			return err
		}
	}
}

func (c *Consumer) processNextBatch(ctx context.Context, queue string) error {
	return c.tx(ctx, func(tx *sql.Tx) error {
		batchID, err := c.nextBatch(ctx, tx, queue)
		if err != nil {
			return errors.Wrap(err, "failed to fetch next batch id")
		}

		if !batchID.Valid {
			return errBatchNotFound
		}

		events, err := c.getBatchEvents(ctx, tx, batchID.Int64)
		if err != nil {
			return errors.Wrap(err, "failed to fetch batch events")
		}

		retry, err := c.processor.Process(ctx, int(batchID.Int64), events)
		if err != nil {
			return errors.Wrap(err, "failed to process batch events")
		}

		for _, event := range retry {
			err = c.retryEvent(ctx, tx, batchID.Int64, event.EventID, event.Delay)
			if err != nil && err != ErrAlreadyExists {
				return errors.Wrap(err, "failed to mark event to retry")
			}
		}

		err = c.finishBatch(ctx, tx, batchID.Int64)
		if err != nil {
			return errors.Wrap(err, "failed to finish")
		}

		return nil
	})
}

// nextBatch used to receive next batch ID for consumer.
// @see https://github.com/markokr/skytools/blob/master/sql/pgq/functions/pgq.next_batch.sql
func (c *Consumer) nextBatch(ctx context.Context, tx *sql.Tx, queue string) (sql.NullInt64, error) {
	const q = `select pgq.next_batch($1, $2) as batch_id;`

	row := tx.QueryRowContext(ctx, q, queue, c.name)

	var batchID sql.NullInt64
	err := row.Scan(&batchID)
	if err != nil {
		return sql.NullInt64{}, errors.Wrap(err, "failed to scan")
	}

	return batchID, nil
}

// getBatchEvents used to receive batch data.
// @see https://github.com/markokr/skytools/blob/master/sql/pgq/functions/pgq.get_batch_events.sql
func (c *Consumer) getBatchEvents(ctx context.Context, tx *sql.Tx, batchID int64) ([]Event, error) {
	const q = `select 
		ev_id,
		ev_time,
		ev_txid,
		ev_retry,
		ev_type,
		ev_data,
		ev_extra1,
		ev_extra2,
		ev_extra3,
		ev_extra4
	from pgq.get_batch_events($1);`

	rows, err := tx.QueryContext(ctx, q, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close() // nolint: errcheck

	var events []Event
	for rows.Next() {
		var e Event

		err = rows.Scan(&e.ID, &e.Time, &e.TxID, &e.Retry, &e.Type, &e.Data, &e.Extra1, &e.Extra2, &e.Extra3, &e.Extra4)
		if err != nil {
			return nil, err
		}

		events = append(events, e)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return events, nil
}

// retryEvent used to send event from batch ID to retry queue.
// @see https://github.com/markokr/skytools/blob/master/sql/pgq/functions/pgq.event_retry.sql
func (c *Consumer) retryEvent(ctx context.Context, tx *sql.Tx, batchID int64, eventID int64, delay time.Duration) error {
	const q = `select pgq.event_retry($1, $2, $3) as st;`

	row := tx.QueryRowContext(ctx, q, batchID, eventID, int(delay.Seconds()))

	// st shows state of operations:
	// 0  - success
	// 1  - event already in retry queue
	var st int
	err := row.Scan(&st)
	if err != nil {
		return errors.Wrap(err, "failed to scan")
	}

	if st == 1 {
		return ErrAlreadyExists
	}

	return nil
}

// finishBatch used to mark batch as processed.
// @see https://github.com/markokr/skytools/blob/master/sql/pgq/functions/pgq.finish_batch.sql
func (c *Consumer) finishBatch(ctx context.Context, tx *sql.Tx, batchID int64) error {
	const q = `select pgq.finish_batch($1) as st;`

	row := tx.QueryRowContext(ctx, q, batchID)

	// st shows state of operations: 1 if batch was found, 0 otherwise.
	var st int
	err := row.Scan(&st)
	if err != nil {
		return errors.Wrap(err, "failed to scan")
	}

	if st == 0 {
		return errBatchNotFound
	}

	return nil
}

// fetchNumber start transaction in which q will executed and result returned as int.
func (c *Consumer) fetchNumber(ctx context.Context, q string, args ...interface{}) (int, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errors.Wrap(err, "failed to start transaction")
	}

	row := tx.QueryRowContext(ctx, q, args...)

	var num int
	err = row.Scan(&num)
	if err != nil {
		if rbe := tx.Rollback(); rbe != nil {
			return 0, errors.Wrap(rbe, "scanning failed and error received on rollback")
		}

		return 0, errors.Wrap(err, "failed to scan")
	}

	err = tx.Commit()
	if err != nil {
		if rbe := tx.Rollback(); rbe != nil {
			return 0, errors.Wrap(rbe, "commit failed and error received on rollback")
		}

		return 0, errors.Wrap(err, "failed to commit")
	}

	return num, nil
}

type txFunc func(tx *sql.Tx) error

// fetchNumber start transaction in which q will executed and result returned as int.
func (c *Consumer) tx(ctx context.Context, f txFunc) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("failed to begin")
	}

	err = f(tx)
	if err != nil {
		if rbe := tx.Rollback(); rbe != nil {
			return errors.Wrap(rbe, "action failed and error received on rollback")
		}

		return err
	}

	err = tx.Commit()
	if err != nil {
		if rbe := tx.Rollback(); rbe != nil {
			return errors.Wrap(rbe, "commit failed and error received on rollback")
		}

		return errors.Wrap(err, "failed to commit")
	}

	return nil
}
