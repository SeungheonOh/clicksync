package model

import (
	"reflect"
	"strings"
	"testing"
)

func TestPersistedFactModelHasOnlyAllowedCBORFields(t *testing.T) {
	allowed := map[string]bool{
		"DatumBody.CBOR":    false,
		"Redeemer.DataCBOR": false,
		"Metadata.CBOR":     false,
	}
	visited := make(map[reflect.Type]bool)
	var inspect func(reflect.Type)
	inspect = func(value reflect.Type) {
		for value.Kind() == reflect.Pointer || value.Kind() == reflect.Slice {
			value = value.Elem()
		}
		if value.Kind() != reflect.Struct ||
			value.PkgPath() != reflect.TypeOf(Block{}).PkgPath() ||
			visited[value] {
			return
		}
		visited[value] = true
		for index := range value.NumField() {
			field := value.Field(index)
			key := value.Name() + "." + field.Name
			if strings.Contains(strings.ToLower(field.Name), "cbor") {
				if _, ok := allowed[key]; !ok {
					t.Errorf("forbidden persisted CBOR field %s", key)
				} else {
					allowed[key] = true
				}
			}
			inspect(field.Type)
		}
	}
	inspect(reflect.TypeOf(Block{}))
	for field, found := range allowed {
		if !found {
			t.Errorf("expected allowed CBOR field %s was not found", field)
		}
	}
}

func TestFactModelHasNoValidationClaims(t *testing.T) {
	if _, found := reflect.TypeOf(Input{}).FieldByName("SourceResolved"); found {
		t.Fatal("Input unexpectedly claims source resolution")
	}
	for _, field := range []string{
		"BodyHashVerified",
		"TransactionIDsVerified",
		"FactsDigest",
	} {
		if _, found := reflect.TypeOf(Block{}).FieldByName(field); found {
			t.Fatalf("Block unexpectedly contains validation field %s", field)
		}
	}
}
