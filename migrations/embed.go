// Package migrations owns the deterministic ClickHouse schema.
package migrations

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"strings"
)

// Initial is the complete idempotent schema migration.
//
//go:embed 001_initial.sql
var Initial string

// Contract is a compact golden descriptor used by compatibility tests and
// external schema consumers.
//
//go:embed schema_contract.txt
var Contract string

// SchemaHash identifies the exact embedded schema bytes.
var SchemaHash = sha256.Sum256([]byte(Initial))

// SplitSQL splits migration text without treating semicolons inside quoted
// strings or comments as statement boundaries.
func SplitSQL(source string) ([]string, error) {
	var statements []string
	var current strings.Builder
	var quote rune
	lineComment := false
	blockComment := false
	runes := []rune(source)
	for index := 0; index < len(runes); index++ {
		character := runes[index]
		var next rune
		if index+1 < len(runes) {
			next = runes[index+1]
		}
		switch {
		case lineComment:
			if character == '\n' {
				lineComment = false
				current.WriteRune(character)
			}
		case blockComment:
			if character == '*' && next == '/' {
				blockComment = false
				index++
			} else if character == '\n' {
				current.WriteRune(character)
			}
		case quote != 0:
			current.WriteRune(character)
			if character == '\\' && index+1 < len(runes) {
				index++
				current.WriteRune(runes[index])
			} else if character == quote {
				if index+1 < len(runes) && runes[index+1] == quote {
					index++
					current.WriteRune(runes[index])
				} else {
					quote = 0
				}
			}
		case character == '-' && next == '-':
			lineComment = true
			index++
		case character == '/' && next == '*':
			blockComment = true
			index++
		case character == '\'' || character == '"' || character == '`':
			quote = character
			current.WriteRune(character)
		case character == ';':
			if statement := strings.TrimSpace(current.String()); statement != "" {
				statements = append(statements, statement)
			}
			current.Reset()
		default:
			current.WriteRune(character)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated SQL quote %q", quote)
	}
	if blockComment {
		return nil, fmt.Errorf("unterminated SQL block comment")
	}
	if statement := strings.TrimSpace(current.String()); statement != "" {
		statements = append(statements, statement)
	}
	return statements, nil
}
