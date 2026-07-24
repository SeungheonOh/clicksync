package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type fakeConnection struct {
	mu sync.Mutex

	execs          []string
	queries        []string
	prepares       map[string]int
	sends          map[string]int
	sendOrder      []string
	batches        map[string][]*fakeBatch
	sendErrors     map[string]error
	prepareErrors  map[string]error
	persistOnError map[string]bool

	datasetRows          [][]any
	publicationHighWater uint64
	eventHighWater       uint64

	queryFn func(string, []any) ([][]any, error)

	blockFactSends  chan struct{}
	factSendEntered chan string
	activeFactSends int
	maxFactSends    int

	pingErr error
	closed  bool
}

func newFakeConnection() *fakeConnection {
	return &fakeConnection{
		prepares:       make(map[string]int),
		sends:          make(map[string]int),
		batches:        make(map[string][]*fakeBatch),
		sendErrors:     make(map[string]error),
		prepareErrors:  make(map[string]error),
		persistOnError: make(map[string]bool),
	}
}

func (c *fakeConnection) Exec(
	_ context.Context,
	query string,
	_ ...any,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, query)
	return nil
}

func (c *fakeConnection) PrepareBatch(
	_ context.Context,
	query string,
) (batch, error) {
	key := insertKey(query, nil)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.prepareErrors[key]; err != nil {
		return nil, err
	}
	value := &fakeBatch{connection: c, query: query, key: key}
	c.prepares[key]++
	c.batches[key] = append(c.batches[key], value)
	return value, nil
}

func (c *fakeConnection) Query(
	_ context.Context,
	query string,
	args ...any,
) (rows, error) {
	c.mu.Lock()
	c.queries = append(c.queries, query)
	fn := c.queryFn
	dataset := cloneRows(c.datasetRows)
	c.mu.Unlock()
	if fn != nil {
		data, err := fn(query, args)
		return &fakeRows{data: cloneRows(data)}, err
	}
	if sameSQL(query, loadDatasetSQL) {
		return &fakeRows{data: dataset}, nil
	}
	return &fakeRows{}, nil
}

func (c *fakeConnection) QueryRow(
	_ context.Context,
	query string,
	_ ...any,
) row {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case sameSQL(query, publicationHighWaterSQL):
		return fakeRow{values: []any{c.publicationHighWater}}
	case sameSQL(query, eventHighWaterSQL):
		return fakeRow{values: []any{c.eventHighWater}}
	default:
		return fakeRow{err: fmt.Errorf("unexpected QueryRow: %s", query)}
	}
}

func (c *fakeConnection) Ping(context.Context) error {
	return c.pingErr
}

func (c *fakeConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *fakeConnection) rowsFor(key string) [][]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ret [][]any
	for _, batch := range c.batches[key] {
		ret = append(ret, cloneRows(batch.rows)...)
	}
	return ret
}

func (c *fakeConnection) sendCount(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sends[key]
}

func (c *fakeConnection) queryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queries)
}

func (c *fakeConnection) onSend(batch *fakeBatch) error {
	key := insertKey(batch.query, batch.rows)
	isFact := key != "dataset" &&
		key != "adoption" &&
		key != "invalidation" &&
		key != "rollbacks"
	c.mu.Lock()
	c.sends[key]++
	c.sendOrder = append(c.sendOrder, key)
	if key != batch.key {
		c.batches[key] = append(c.batches[key], batch)
	}
	err := c.sendErrors[key]
	block := c.blockFactSends
	entered := c.factSendEntered
	if isFact && block != nil {
		c.activeFactSends++
		if c.activeFactSends > c.maxFactSends {
			c.maxFactSends = c.activeFactSends
		}
	}
	if key == "dataset" && (err == nil || c.persistOnError[key]) {
		c.datasetRows = append(c.datasetRows, cloneRows(batch.rows)...)
	}
	c.mu.Unlock()
	if isFact && block != nil {
		if entered != nil {
			entered <- key
		}
		<-block
		c.mu.Lock()
		c.activeFactSends--
		c.mu.Unlock()
	}
	return err
}

type fakeBatch struct {
	connection *fakeConnection
	query      string
	key        string
	rows       [][]any
	aborted    bool
}

func (b *fakeBatch) Append(values ...any) error {
	if b.aborted {
		return errors.New("append after abort")
	}
	b.rows = append(b.rows, append([]any(nil), values...))
	return nil
}

func (b *fakeBatch) Send() error {
	if b.aborted {
		return errors.New("send after abort")
	}
	return b.connection.onSend(b)
}

func (b *fakeBatch) Abort() error {
	b.aborted = true
	return nil
}

type fakeRows struct {
	data  [][]any
	index int
	err   error
}

func (r *fakeRows) Next() bool {
	return r.index < len(r.data)
}

func (r *fakeRows) Scan(destinations ...any) error {
	if r.index >= len(r.data) {
		return errors.New("scan without row")
	}
	values := r.data[r.index]
	r.index++
	return scanValues(destinations, values)
}

func (r *fakeRows) Err() error {
	return r.err
}

func (r *fakeRows) Close() error {
	return nil
}

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(destinations ...any) error {
	if r.err != nil {
		return r.err
	}
	return scanValues(destinations, r.values)
}

func scanValues(destinations, values []any) error {
	if len(destinations) != len(values) {
		return fmt.Errorf(
			"scan destination count %d, values %d",
			len(destinations),
			len(values),
		)
	}
	for index, destination := range destinations {
		target := reflect.ValueOf(destination)
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return fmt.Errorf("scan destination %d is not a pointer", index)
		}
		target = target.Elem()
		if values[index] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		value := reflect.ValueOf(values[index])
		if value.Type().AssignableTo(target.Type()) {
			target.Set(value)
			continue
		}
		if value.Type().ConvertibleTo(target.Type()) {
			target.Set(value.Convert(target.Type()))
			continue
		}
		if target.Kind() == reflect.Pointer &&
			value.Type().AssignableTo(target.Type().Elem()) {
			pointer := reflect.New(target.Type().Elem())
			pointer.Elem().Set(value)
			target.Set(pointer)
			continue
		}
		return fmt.Errorf(
			"scan value %d type %s is not assignable to %s",
			index,
			value.Type(),
			target.Type(),
		)
	}
	return nil
}

func insertKey(query string, rows [][]any) string {
	fields := strings.Fields(query)
	for index := range fields {
		if strings.EqualFold(fields[index], "INTO") && index+1 < len(fields) {
			table := strings.TrimPrefix(fields[index+1], "clicksync.")
			if table == "chain_events" && len(rows) > 0 && len(rows[0]) > 2 {
				if kind, ok := rows[0][2].(string); ok {
					return kind
				}
			}
			return table
		}
	}
	return ""
}

func sameSQL(left, right string) bool {
	return strings.Join(strings.Fields(left), " ") ==
		strings.Join(strings.Fields(right), " ")
}

func cloneRows(rows [][]any) [][]any {
	ret := make([][]any, len(rows))
	for index := range rows {
		ret[index] = append([]any(nil), rows[index]...)
	}
	return ret
}

type fakeLock struct {
	mu     sync.Mutex
	calls  int
	failAt int
}

func (l *fakeLock) AssertHeld() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.failAt > 0 && l.calls >= l.failAt {
		return errors.New("lock lost")
	}
	return nil
}
