package mysqlutils

import (
	"github.com/go-sql-driver/mysql"
)

const (
	MYSQL_ERROR_LOCK_DEADLOCK = 1213
)

func MysqlIsErrorIn(err error, expectedErrorNumbers ...int) bool {
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
