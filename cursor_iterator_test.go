package cursoriterator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	cursoriterator "github.com/Vadim89/go-pgx-cursor-iterator"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stretchr/testify/require"
)

// make sure PgxConnector implements pgxpool.Pool.
var _ cursoriterator.PgxConnector = &pgxpool.Pool{}

// make sure PgxConnector implements pgx.Conn.
var _ cursoriterator.PgxConnector = &pgx.Conn{}

type User struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func runTest(t *testing.T, usersToInsert []User, fn func(pool *pgxpool.Pool)) {
	testDB := NewTestDatabase(t)
	defer testDB.Close(t)

	pool, err := pgxpool.New(context.Background(), testDB.ConnectionString(t))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(context.Background(), `
CREATE TABLE users (
	id		integer PRIMARY KEY,
	name	varchar(32) NOT NULL
)`)
	require.NoError(t, err)

	for _, user := range usersToInsert {
		_, err = pool.Exec(context.Background(), "INSERT INTO users VALUES($1, $2)", user.ID, user.Name)
		require.NoError(t, err)
	}

	fn(pool)
}

func expectValues(t *testing.T, iter *cursoriterator.CursorIterator, values []User, expected ...User) {
	for _, user := range expected {
		require.True(t, iter.Next())
		require.NoError(t, iter.Error())
		require.Equal(t, user, values[iter.ValueIndex()])
	}
	require.False(t, iter.Next())
	require.NoError(t, iter.Error())
}

func TestValueIndex(t *testing.T) {
	t.Parallel()
	runTest(
		t,
		[]User{
			{1, "Joe"},
			{2, "Alice"},
			{3, "Bob"},
			{4, "Mike"},
			{5, "Maria"},
		},
		func(pool *pgxpool.Pool) {
			values := make([]User, 2)
			iter, err := cursoriterator.NewCursorIterator(context.Background(), pool, values, time.Minute, "SELECT * FROM users")
			require.NoError(t, err)
			require.Equal(t, -2, iter.ValueIndex())
			require.True(t, iter.Next())
			require.NoError(t, iter.Error())
			require.Equal(t, 0, iter.ValueIndex())
			require.True(t, iter.Next())
			require.NoError(t, iter.Error())
			require.Equal(t, 1, iter.ValueIndex())
			require.True(t, iter.Next())
			require.NoError(t, iter.Error())
			require.Equal(t, 0, iter.ValueIndex())
			require.True(t, iter.Next())
			require.NoError(t, iter.Error())
			require.Equal(t, 1, iter.ValueIndex())
			require.True(t, iter.Next())
			require.NoError(t, iter.Error())
			require.Equal(t, 0, iter.ValueIndex())
			require.False(t, iter.Next())
			require.NoError(t, iter.Error())
			require.Equal(t, -1, iter.ValueIndex())
			require.False(t, iter.Next())
			require.NoError(t, iter.Error())
			require.Equal(t, -1, iter.ValueIndex())
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
			require.NoError(t, iter.Error())
		})
}
func TestCacheSizes(t *testing.T) {
	t.Parallel()
	cacheSizes := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for _, size := range cacheSizes {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			t.Parallel()
			runTest(
				t,
				[]User{
					{1, "Joe"},
					{2, "Alice"},
					{3, "Bob"},
					{4, "Mike"},
					{5, "Maria"},
				},
				func(pool *pgxpool.Pool) {
					values := make([]User, size)
					iter, err := cursoriterator.NewCursorIterator(context.Background(), pool, values, time.Minute, "SELECT * FROM users")
					require.NoError(t, err)

					expectValues(t, iter, values,
						User{1, "Joe"},
						User{2, "Alice"},
						User{3, "Bob"},
						User{4, "Mike"},
						User{5, "Maria"},
					)
					require.NoError(t, iter.Close())
				})
		})
	}
}

func TestEmptyTable(t *testing.T) {
	t.Parallel()
	runTest(
		t,
		[]User{},
		func(pool *pgxpool.Pool) {
			values := make([]User, 3)
			iter, err := cursoriterator.NewCursorIterator(context.Background(), pool, values, time.Minute, "SELECT * FROM users")
			require.NoError(t, err)

			expectValues(t, iter, values)
			require.NoError(t, iter.Close())
		})
}

func TestNextAfterClose(t *testing.T) {
	t.Parallel()
	runTest(
		t,
		[]User{
			{1, "Joe"},
			{2, "Alice"},
			{3, "Bob"},
			{4, "Mike"},
			{5, "Maria"},
		},
		func(pool *pgxpool.Pool) {
			values := make([]User, 3)
			iter, err := cursoriterator.NewCursorIterator(context.Background(), pool, values, time.Minute, "SELECT * FROM users")
			require.NoError(t, err)

			require.True(t, iter.Next())
			require.Equal(t, 0, iter.ValueIndex())
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
			require.Equal(t, -1, iter.ValueIndex())
			require.NoError(t, iter.Error())
			require.False(t, iter.Next())
			require.Equal(t, -1, iter.ValueIndex())
		})
}

func TestInvalidConstructorParameters(t *testing.T) {
	t.Parallel()

	t.Run("connector cannot be nil", func(t *testing.T) {
		t.Parallel()
		iter, err := cursoriterator.NewCursorIterator(context.Background(), nil, make([]User, 3), time.Minute, "SELECT * FROM users")
		require.EqualError(t, err, "connector cannot be nil")
		require.Nil(t, iter)
	})

	t.Run("values cannot be nil", func(t *testing.T) {
		t.Parallel()
		iter, err := cursoriterator.NewCursorIterator(context.Background(), &pgxpool.Pool{}, nil, time.Minute, "SELECT * FROM users")
		require.EqualError(t, err, "values cannot be nil")
		require.Nil(t, iter)
	})

	t.Run("vales must be a slice", func(t *testing.T) {
		t.Parallel()
		iter, err := cursoriterator.NewCursorIterator(context.Background(), &pgxpool.Pool{}, User{}, time.Minute, "SELECT * FROM users")
		require.EqualError(t, err, "values must be a slice")
		require.Nil(t, iter)
	})

	t.Run("values must have a capacity bigger than 0", func(t *testing.T) {
		t.Parallel()
		iter, err := cursoriterator.NewCursorIterator(context.Background(), &pgxpool.Pool{}, make([]User, 0), time.Minute, "SELECT * FROM users")
		require.EqualError(t, err, "values must have a capacity bigger than 0")
		require.Nil(t, iter)
	})
}

func TestTimeout(t *testing.T) {
	t.Parallel()
	runTest(
		t,
		[]User{
			{1, "Joe"},
			{2, "Alice"},
			{3, "Bob"},
			{4, "Mike"},
			{5, "Maria"},
		},
		func(pool *pgxpool.Pool) {
			values := make([]User, 1)
			iter, err := cursoriterator.NewCursorIterator(context.Background(), pool, values, time.Second, "SELECT * FROM users")
			require.NoError(t, err)

			require.True(t, iter.Next())
			require.Equal(t, 0, iter.ValueIndex())
			require.NoError(t, iter.Error())

			time.Sleep(time.Second * 3)
			require.True(t, iter.Next())
			require.Equal(t, 0, iter.ValueIndex())
			require.NoError(t, iter.Error())
			require.NoError(t, iter.Close())
		})
}
