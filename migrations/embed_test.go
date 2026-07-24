package migrations

import (
	"crypto/sha256"
	"slices"
	"strings"
	"testing"
)

func TestEmbeddedSchemaHash(t *testing.T) {
	t.Parallel()
	if Initial == "" {
		t.Fatal("embedded migration is empty")
	}
	if got := sha256.Sum256([]byte(Initial)); got != SchemaHash {
		t.Fatalf("SchemaHash = %x, want %x", SchemaHash, got)
	}
	if strings.Contains(Initial, "dataset_manifest") ||
		strings.Contains(Initial, "peer_observations") ||
		strings.Contains(Initial, "trust_") ||
		strings.Contains(Initial, "evidence_") {
		t.Fatal("schema contains removed manifest/trust/evidence state")
	}
}

func TestGoldenDescriptorCoversEveryTable(t *testing.T) {
	t.Parallel()
	for _, table := range []string{
		"dataset",
		"blocks",
		"transactions",
		"inputs",
		"outputs",
		"datum_bodies",
		"datum_observations",
		"withdrawals",
		"redeemers",
		"transaction_metadata",
		"chain_events",
		"rollbacks",
	} {
		if !strings.Contains(Contract, table+"|") {
			t.Errorf("golden descriptor omits %s", table)
		}
		if !strings.Contains(Initial, "clicksync."+table+"\n") {
			t.Errorf("migration omits %s", table)
		}
	}
}

func TestGoldenDescriptorMatchesMigrationColumns(t *testing.T) {
	t.Parallel()
	for _, line := range strings.Split(strings.TrimSpace(Contract), "\n") {
		table, descriptor, ok := strings.Cut(line, "|")
		if !ok {
			t.Fatalf("malformed descriptor line %q", line)
		}
		want := strings.Split(descriptor, ",")
		got := migrationColumns(t, table)
		if !slices.Equal(got, want) {
			t.Errorf("%s columns:\n got %q\nwant %q", table, got, want)
		}
	}
}

func migrationColumns(t *testing.T, table string) []string {
	t.Helper()
	marker := "CREATE TABLE IF NOT EXISTS clicksync." + table + "\n("
	start := strings.Index(Initial, marker)
	if start < 0 {
		t.Fatalf("migration omits table %s", table)
	}
	body := Initial[start+len(marker):]
	end := strings.Index(body, "\n)\nENGINE")
	if end < 0 {
		t.Fatalf("cannot find end of table %s", table)
	}
	body = body[:end]
	var columns []string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "    ") ||
			strings.HasPrefix(line, "        ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "INDEX", "PROJECTION", "CONSTRAINT":
			continue
		}
		if (fields[0][0] < 'a' || fields[0][0] > 'z') &&
			(fields[0][0] < 'A' || fields[0][0] > 'Z') {
			continue
		}
		columns = append(columns, strings.TrimSuffix(fields[0], ","))
	}
	return columns
}

func TestSplitSQL(t *testing.T) {
	t.Parallel()
	source := `
-- leading ; comment
CREATE TABLE x (value String DEFAULT ';');
/* a ; block comment */
CREATE TABLE y (value String DEFAULT 'it''s;fine');
`
	statements, err := SplitSQL(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 2 {
		t.Fatalf("got %d statements: %#v", len(statements), statements)
	}
	for _, statement := range statements {
		if strings.HasSuffix(statement, ";") {
			t.Fatalf("statement retains separator: %q", statement)
		}
	}
}

func TestSplitSQLRejectsUnterminatedTokens(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"SELECT 'x", "SELECT 1 /* x"} {
		if _, err := SplitSQL(source); err == nil {
			t.Fatalf("SplitSQL(%q) succeeded", source)
		}
	}
}

func TestMigrationIsIdempotentDDL(t *testing.T) {
	t.Parallel()
	statements, err := SplitSQL(Initial)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 13 {
		t.Fatalf("got %d statements, want database plus 12 tables", len(statements))
	}
	for _, statement := range statements {
		upper := strings.ToUpper(strings.TrimSpace(statement))
		if !strings.HasPrefix(upper, "CREATE DATABASE IF NOT EXISTS") &&
			!strings.HasPrefix(upper, "CREATE TABLE IF NOT EXISTS") {
			t.Fatalf("migration statement is not idempotent CREATE: %.80q", statement)
		}
	}
}
