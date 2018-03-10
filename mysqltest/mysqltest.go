package mysqltest

// MYSQL_HOSTS=localhost MYSQL_USERNAME=test_user MYSQL_PASSWORD=test_password MYSQL_DATABASE=test_db ginkgo

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	. "github.com/onsi/gomega"
)

var MysqlHosts string
var MysqlUsername string
var MysqlPassword string
var MysqlDatabase string
var ConnStr string
var ConnStrWithDb string
var DB *sql.DB
var TX *sql.Tx

type TestingConfig struct {
	HostsEnvKey      string
	UsernameEnvKey   string
	PasswordEnvKey   string
	DatabaseEnvKey   string
	ReplicatedEnvKey string
}

func DatabaseEnvKey(key string) func(cfg *TestingConfig) {
	return func(cfg *TestingConfig) {
		cfg.DatabaseEnvKey = key
	}
}

func DeleteData() {
	tables := []string{}

	rows, err := DB.Query("SHOW TABLES")
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			Expect(err).NotTo(HaveOccurred())
		}
		tables = append(tables, table)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())

	for _, table := range tables {
		_, err = DB.Exec("DELETE FROM `" + table + "`")
		Expect(rows.Err()).NotTo(HaveOccurred())
	}
}

func RefreshTx() {
	var err error

	if TX != nil {
		err = TX.Commit()
		Expect(err).NotTo(HaveOccurred())
	}

	TX, err = DB.Begin()
	Expect(err).NotTo(HaveOccurred())
}

func MysqlInitTesting(t *testing.T, opts ...func(*TestingConfig)) bool {
	cfg := &TestingConfig{
		HostsEnvKey:      "MYSQL_HOSTS",
		UsernameEnvKey:   "MYSQL_USERNAME",
		PasswordEnvKey:   "MYSQL_PASSWORD",
		DatabaseEnvKey:   "MYSQL_DATABASE",
		ReplicatedEnvKey: "MYSQL_REPLICATED",
	}

	for _, option := range opts {
		option(cfg)
	}

	MysqlHosts = os.Getenv(cfg.HostsEnvKey)
	MysqlUsername = os.Getenv(cfg.UsernameEnvKey)
	MysqlPassword = os.Getenv(cfg.PasswordEnvKey)
	MysqlDatabase = os.Getenv(cfg.DatabaseEnvKey)
	mysqlReplicated := os.Getenv(cfg.ReplicatedEnvKey) == "true"

	if MysqlHosts == "" || MysqlUsername == "" || MysqlPassword == "" || MysqlDatabase == "" {
		t.Skip(fmt.Sprintf("Missing %s, %s, %s, %s env variables", cfg.HostsEnvKey, cfg.UsernameEnvKey, cfg.PasswordEnvKey, cfg.DatabaseEnvKey))
		return false
	}

	if !strings.Contains(MysqlHosts, ":") {
		MysqlHosts = MysqlHosts + ":3306"
	}

	ConnStr = fmt.Sprintf("%s:%s@(%s)/", MysqlUsername, MysqlPassword, MysqlHosts)
	ConnStrWithDb = ConnStr + MysqlDatabase

	if mysqlReplicated {
		ConnStr += "?wsrep_causal_reads=1"
		ConnStrWithDb += "?wsrep_causal_reads=1"
	}

	return true
}

func MysqlBeforeSuite() {
	initialDb, err := sql.Open("mysql", ConnStr)
	Expect(err).NotTo(HaveOccurred())

	_, err = initialDb.Exec("CREATE DATABASE IF NOT EXISTS `" + MysqlDatabase + "`")
	Expect(err).NotTo(HaveOccurred())

	initialDb.Close()

	DB, err = sql.Open("mysql", ConnStrWithDb)
	Expect(err).NotTo(HaveOccurred())
}

func MysqlBeforeEach() {
	DeleteData()

	TX = nil

	RefreshTx()
}

func MysqlAfterEach() {
	err := TX.Commit()
	Expect(err).NotTo(HaveOccurred())

	TX = nil
}
