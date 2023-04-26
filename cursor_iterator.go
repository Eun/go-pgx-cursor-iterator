// Package cursoriterator provides functionality to iterate over big batches of postgres rows.
package cursoriterator

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"

	"github.com/jackc/pgx/v5"

	"github.com/pkg/errors"
)

// CursorIterator will be returned by NewCursorIterator().
// It provides functionality to loop over postgres rows and
// holds all necessary internal information for the functionality.
type CursorIterator struct {
	ctx                      context.Context
	connector                PgxConnector
	maxDatabaseExecutionTime time.Duration
	query                    string
	args                     []interface{}

	fetchQuery string

	values       []interface{}
	valuesPos    int
	valuesMaxPos int

	err error

	tx pgx.Tx

	mu sync.Mutex
}

// PgxConnector implements the Begin() function from the pgx and pgxpool packages.
type PgxConnector interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// NewCursorIterator can be used to create a new iterator.
// Required parameters:
//
//	connector                 most likely a *pgx.Conn or *pgxpool.Pool, needed to start a transaction on the database
//	values                    a slice where the fetched values should be stored in.
//	maxDatabaseExecutionTime  how long should one database operation be allowed to run.
//	query                     the query to fetch the rows
//	args                      arguments for the query
//
// Example Usage:
//
//	 ctx := context.Background()
//		values := make([]User, 1000)
//		iter, err := NewCursorIterator(ctx, pool, values, time.Minute, "SELECT * FROM users WHERE role = $1", "Guest")
//		if err != nil {
//			panic(err)
//		}
//		defer iter.Close()
//		for iter.Next() {
//			fmt.Printf("Name: %s\n", values[iter.ValueIndex()].Name)
//		}
//		if err := iter.Error(); err != nil {
//			panic(err)
//		}
func NewCursorIterator(
	ctx context.Context,
	connector PgxConnector,
	values interface{},
	maxDatabaseExecutionTime time.Duration,
	query string, args ...interface{},
) (*CursorIterator, error) {
	if connector == nil {
		return nil, errors.New("connector cannot be nil")
	}
	if values == nil {
		return nil, errors.New("values cannot be nil")
	}
	rv := reflect.ValueOf(values)
	if !rv.IsValid() {
		return nil, errors.New("values is invalid")
	}

	if rv.Kind() != reflect.Slice {
		return nil, errors.New("values must be a slice")
	}

	valuesCapacity := rv.Cap()

	if valuesCapacity <= 0 {
		return nil, errors.New("values must have a capacity bigger than 0")
	}

	valuesSlice := make([]interface{}, valuesCapacity)
	for i := 0; i < valuesCapacity; i++ {
		elem := rv.Index(i)
		if !elem.CanAddr() {
			return nil, errors.Errorf("unable to reference %s", elem.Type().String())
		}
		elem = elem.Addr()
		if !elem.CanInterface() {
			return nil, errors.Errorf("unable to get interface of %s", elem.Type().String())
		}
		valuesSlice[i] = elem.Interface()
	}

	return &CursorIterator{
		ctx:                      ctx,
		connector:                connector,
		maxDatabaseExecutionTime: maxDatabaseExecutionTime,
		query:                    query,
		args:                     args,

		fetchQuery: fmt.Sprintf("FETCH %d IN curs", valuesCapacity),

		values:       valuesSlice,
		valuesPos:    -2,
		valuesMaxPos: valuesCapacity - 1,

		err: nil,

		tx: nil,
	}, nil
}

func (iter *CursorIterator) fetchNextRows() {
	ctx, cancel := context.WithTimeout(iter.ctx, iter.maxDatabaseExecutionTime)
	defer cancel()

	rows, err := iter.tx.Query(ctx, iter.fetchQuery)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			iter.close()
			return
		}
		iter.err = err
		return
	}

	scanner := pgxscan.NewRowScanner(rows)

	i := 0
	for rows.Next() {
		if i > iter.valuesMaxPos {
			iter.close()
			iter.err = errors.New("database returned more rows than expected")
			return
		}
		if err := scanner.Scan(iter.values[i]); err != nil {
			iter.close()
			iter.err = errors.Wrap(err, "unable to scan into values element")
			return
		}
		i++
	}

	if err := rows.Err(); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			iter.close()
			return
		}
		iter.close()
		iter.err = errors.Wrap(err, "unable to fetch rows")
		return
	}
	if i == 0 {
		iter.close()
		return
	}
	iter.valuesPos = 0
	iter.valuesMaxPos = i
}

// Next will return true if there is a next value available, false if there is no next value available.
// Next will also fetch next values when all current values have been iterated.
func (iter *CursorIterator) Next() bool {
	iter.mu.Lock()
	defer iter.mu.Unlock()
	// it is not the first row, and we already iterated over all rows: early exit
	if iter.valuesPos == -1 {
		return false
	}

	if iter.valuesPos == -2 {
		// first call:
		// start a transaction
		// and declare the cursor
		ctx, cancel := context.WithTimeout(iter.ctx, iter.maxDatabaseExecutionTime)
		defer cancel()

		// start a transaction
		iter.tx, iter.err = iter.connector.Begin(ctx)
		if iter.err != nil {
			iter.err = errors.Wrap(iter.err, "unable to start transaction")
			return false
		}

		// declare cursor
		if _, err := iter.tx.Exec(ctx, "DECLARE curs CURSOR FOR "+iter.query, iter.args...); err != nil {
			iter.close()
			iter.err = errors.Wrap(err, "unable to declare cursor")
			return false
		}
		// fetch the initial rows
		iter.fetchNextRows()
		// return true if we have rows
		return iter.valuesPos == 0
	}

	// do we still have items in the cache?
	if iter.valuesPos+1 < iter.valuesMaxPos {
		iter.valuesPos++
		return true
	}

	// we hit the end: fetch the next chunk of rows
	iter.fetchNextRows()
	return iter.valuesPos == 0
}

// ValueIndex will return the current value index that can be used to fetch the current value.
// Notice that it will return values below 0 when there is no next value available or the iteration didn't started yet.
func (iter *CursorIterator) ValueIndex() int {
	iter.mu.Lock()
	i := iter.valuesPos
	iter.mu.Unlock()
	return i
}

// Error will return the last error that appeared during fetching.
func (iter *CursorIterator) Error() error {
	iter.mu.Lock()
	err := iter.err
	iter.mu.Unlock()
	return err
}

func (iter *CursorIterator) close() {
	if iter.tx == nil {
		iter.err = nil
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), iter.maxDatabaseExecutionTime)
	iter.err = iter.tx.Rollback(ctx)
	iter.tx = nil
	cancel()
	iter.valuesPos = -1
}

// Close will close the iterator and all Next() calls will return false.
// After Close the iterator is unusable and can not be used again.
func (iter *CursorIterator) Close() error {
	iter.mu.Lock()
	defer iter.mu.Unlock()
	iter.close()
	return iter.err
}
