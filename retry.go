package mysqlutils

import (
	"database/sql"
	"math/rand"
	"time"
)

type DB interface {
	Begin() (*sql.Tx, error)
}

func MysqlRetry(db DB, retries int, expectedErrorNumbers ...int) func(f func(*sql.Tx) error) error {
	return func(f func(*sql.Tx) error) error {
		var lastErr error

		// first try is not a retry
		for i := 0; i < retries+1; i++ {
			if i > 0 {
				time.Sleep(time.Duration(i*rand.Intn(50)) * time.Millisecond)
			}

			tx, err := db.Begin()
			if err != nil {
				if MysqlIsErrorIn(err, expectedErrorNumbers...) {
					lastErr = err
					continue
				} else {
					return err
				}
			}

			if err = f(tx); err != nil {
				if MysqlIsErrorIn(err, expectedErrorNumbers...) {
					lastErr = err
					continue
				} else {
					return err
				}
			}

			if err = tx.Commit(); err != nil {
				if MysqlIsErrorIn(err, expectedErrorNumbers...) {
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

func MysqlRetryDefault(db *sql.DB, retries int) func(f func(*sql.Tx) error) error {
	return MysqlRetry(db, retries, MYSQL_ERROR_LOCK_DEADLOCK)
}
