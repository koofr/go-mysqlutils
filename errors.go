package mysqlutils

import (
	"github.com/go-sql-driver/mysql"
)

const (
	ERROR_DUP_ENTRY         = 1062
	ERROR_LOCK_WAIT_TIMEOUT = 1205
	ERROR_LOCK_DEADLOCK     = 1213
	ERROR_QUERY_INTERRUPTED = 1317
	ERROR_NO_SUCH_TABLE     = 1146
)

func IsErrorIn(err error, expectedErrorNumbers ...int) bool {
	if mysqlErr, ok := err.(*mysql.MySQLError); ok {
		errNumber := int(mysqlErr.Number)

		for _, expectedErrNumber := range expectedErrorNumbers {
			if expectedErrNumber == errNumber {
				return true
			}
		}

		return false
	} else {
		return false
	}
}
