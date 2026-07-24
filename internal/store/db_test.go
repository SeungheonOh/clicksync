package store

import (
	"testing"

	"clicksync/internal/config"
)

func TestClickHouseOptionsKeepInsertsSynchronous(t *testing.T) {
	options := clickHouseOptions(config.Config{
		ClickHouseHost:     "clickhouse",
		ClickHousePort:     9000,
		ClickHouseUser:     "clicksync",
		ClickHousePassword: "secret",
	})
	if got := options.Settings["async_insert"]; got != 0 {
		t.Fatalf("async_insert = %#v, want 0", got)
	}
	if got := options.Settings["wait_for_async_insert"]; got != 1 {
		t.Fatalf("wait_for_async_insert = %#v, want 1", got)
	}
}

func TestSplitSQLPreservesQuotedSemicolon(t *testing.T) {
	got, err := splitSQL("CREATE TABLE x (s String DEFAULT ';'); -- ignored\nSELECT 1;")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("statements = %#v", got)
	}
}
