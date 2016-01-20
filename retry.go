package mysqlutils

import (
	"database/sql"
	"math/rand"
	"time"
)

type DB interface {
	Begin() (*sql.Tx, error)
}

func Retry(db DB, retries int, expectedErrorNumbers ...int) func(f func(*sql.Tx) error) error {
	return func(f func(*sql.Tx) error) error {
		var lastErr error

		// first try is not a retry
		for i := 0; i < retries+1; i++ {
			if i > 0 {
				time.Sleep(time.Duration(i*rand.Intn(50)) * time.Millisecond)
			}

			tx, err := db.Begin()
			if err != nil {
				if IsErrorIn(err, expectedErrorNumbers...) {
					lastErr = err
					continue
				} else {
					return err
				}
			}

			if err = f(tx); err != nil {
				tx.Rollback()

				if IsErrorIn(err, expectedErrorNumbers...) {
					lastErr = err
					continue
				} else {
					return err
				}
			}

			if err = tx.Commit(); err != nil {
				if IsErrorIn(err, expectedErrorNumbers...) {
					lastErr = err
					continue
				} else {
					return err
				}
			}

			return nil
		}

		return lastErr
	}
}

func RetryDefault(db *sql.DB, retries int) func(f func(*sql.Tx) error) error {
	return Retry(db, retries, ERROR_LOCK_DEADLOCK, ERROR_QUERY_INTERRUPTED)
}
