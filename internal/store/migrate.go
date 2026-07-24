package store

import (
	"context"
	"fmt"

	"cardano-clicksync/migrations"
)

func (d *DB) Migrate(ctx context.Context) error {
	if d == nil || d.conn == nil {
		return errorsNewNilDB()
	}
	statements, err := migrations.SplitSQL(migrations.Initial)
	if err != nil {
		return fmt.Errorf("split embedded migration: %w", err)
	}
	for index, statement := range statements {
		if err := d.conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("migration statement %d: %w", index+1, err)
		}
	}
	return nil
}
