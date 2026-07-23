package store

import "testing"

func TestSplitSQLPreservesQuotedSemicolon(t *testing.T) {
	got, err := splitSQL("CREATE TABLE x (s String DEFAULT ';'); -- ignored\nSELECT 1;")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("statements = %#v", got)
	}
}
