package deduplicator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	_ "modernc.org/sqlite" // pure-Go driver; registers the "sqlite" database/sql driver
)

var ErrEmptyDBPath = errors.New("deduplicator: db path is empty")

const defaultRetention = 30 * 24 * time.Hour
const purgeInterval = 24 * time.Hour
const insertChunkSize = 1000

// as TEXT 'YYYY-MM-DD' so lexical comparison matches chronological order.
const schemaDDL = `CREATE TABLE IF NOT EXISTS seen_domains (
    domain    TEXT NOT NULL PRIMARY KEY,
    seen_date TEXT NOT NULL
) WITHOUT ROWID;`

type Options struct {
	DBPath    string
	Retention time.Duration
	CacheSize int // in memory cache size; 0 disables caching
}

type Deduplicator struct {
	db        *sql.DB
	cache     *lru.Cache[string, struct{}] // nil when caching is disabled
	retention time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup

	now func() time.Time // injectable clock for tests
}

//   - journal_mode(WAL):   readers never block the writer (persisted in the file)
//   - busy_timeout(5000):  sleep-retry up to 5s instead of erroring if an
//     external process holds a lock
//   - synchronous(NORMAL): durable under app crash, the correct fast pairing with WAL
//   - _txlock=immediate:   any future explicit write-txn takes the write lock at
//     BEGIN, avoiding deferred->write upgrade deadlocks
func buildDSN(path string) string {
	return path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_txlock=immediate"
}

func New(opts Options) (*Deduplicator, error) {
	if opts.DBPath == "" {
		return nil, ErrEmptyDBPath
	}

	database, err := sql.Open("sqlite", buildDSN(opts.DBPath))
	if err != nil {
		return nil, fmt.Errorf("deduplicator: open db: %w", err)
	}

	// Serialize ALL access (FilterNew lookups/inserts and the retention delete)
	// one caller at a time, so SQLite never sees concurrent statements and cannot
	database.SetMaxOpenConns(1)

	if _, execErr := database.ExecContext(context.Background(), schemaDDL); execErr != nil {
		_ = database.Close()
		return nil, fmt.Errorf("deduplicator: init schema: %w", execErr)
	}

	var cache *lru.Cache[string, struct{}]
	if opts.CacheSize > 0 {
		cache, err = lru.New[string, struct{}](opts.CacheSize)
		if err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("deduplicator: init cache: %w", err)
		}
	}

	retention := opts.Retention
	if retention <= 0 {
		retention = defaultRetention
	}

	ctx, cancel := context.WithCancel(context.Background())
	dedup := &Deduplicator{
		db:        database,
		cache:     cache,
		retention: retention,
		cancel:    cancel,
		now:       time.Now,
	}

	dedup.wg.Add(1)
	go dedup.purgeLoop(ctx)

	return dedup, nil
}

func (d *Deduplicator) FilterNew(ctx context.Context, domains []string) ([]string, error) {
	if len(domains) == 0 {
		return nil, nil
	}

	// Collapse intra-batch duplicates while preserving first-seen order.
	inBatch := make(map[string]struct{}, len(domains))
	unique := make([]string, 0, len(domains))

	for _, domain := range domains {
		if domain == "" {
			continue
		}

		if _, dup := inBatch[domain]; dup {
			continue
		}

		inBatch[domain] = struct{}{}

		unique = append(unique, domain)
	}

	// Drop domains already known-seen via the front cache; the rest need a DB check.
	candidates := unique
	if d.cache != nil {
		candidates = make([]string, 0, len(unique))
		for _, domain := range unique {
			if d.cache.Contains(domain) {
				continue
			}

			candidates = append(candidates, domain)
		}
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	existing, err := d.existing(ctx, candidates)
	if err != nil {
		return nil, err
	}

	newDomains := make([]string, 0, len(candidates))
	for _, domain := range candidates {
		if _, seen := existing[domain]; seen {
			d.cacheAdd(domain)
			continue
		}

		newDomains = append(newDomains, domain)
	}

	if len(newDomains) > 0 {
		if insertErr := d.insert(ctx, newDomains, d.now()); insertErr != nil {
			return nil, insertErr
		}

		for _, domain := range newDomains {
			d.cacheAdd(domain)
		}
	}

	return newDomains, nil
}

func (d *Deduplicator) existing(ctx context.Context, domains []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(domains))

	for start := 0; start < len(domains); start += insertChunkSize {
		end := min(start+insertChunkSize, len(domains))
		chunk := domains[start:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, len(chunk))

		for i, domain := range chunk {
			placeholders[i] = "?"
			args[i] = domain
		}

		query := "SELECT domain FROM seen_domains WHERE domain IN (" + strings.Join(placeholders, ",") + ")"

		queryErr := d.scanExisting(ctx, query, args, result)
		if queryErr != nil {
			return nil, queryErr
		}
	}

	return result, nil
}

func (d *Deduplicator) scanExisting(ctx context.Context, query string, args []any, result map[string]struct{}) error {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("deduplicator: existence query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var domain string
		if scanErr := rows.Scan(&domain); scanErr != nil {
			return fmt.Errorf("deduplicator: scan domain: %w", scanErr)
		}

		result[domain] = struct{}{}
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("deduplicator: iterate rows: %w", rowsErr)
	}

	return nil
}

func (d *Deduplicator) insert(ctx context.Context, domains []string, date time.Time) error {
	dateStr := date.UTC().Format(time.DateOnly)

	for start := 0; start < len(domains); start += insertChunkSize {
		end := min(start+insertChunkSize, len(domains))
		chunk := domains[start:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)*2)

		for i, domain := range chunk {
			placeholders[i] = "(?, ?)"

			args = append(args, domain, dateStr)
		}

		//nolint:gosec // built only from literal placeholders; all values are parameterized
		query := "INSERT OR IGNORE INTO seen_domains (domain, seen_date) VALUES " +
			strings.Join(placeholders, ",")

		if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("deduplicator: insert domains: %w", err)
		}
	}

	return nil
}

// remove all entries with a seen_date strictly older than cutoff
func (d *Deduplicator) purge(ctx context.Context, cutoff time.Time) (int64, error) {
	cutoffStr := cutoff.UTC().Format(time.DateOnly)

	const query = "DELETE FROM seen_domains " +
		"WHERE domain IN (SELECT domain FROM seen_domains WHERE seen_date < ? LIMIT 10000)"

	var total int64

	for {
		select {
		case <-ctx.Done():
			return total, fmt.Errorf("deduplicator: purge canceled: %w", ctx.Err())
		default:
		}

		result, err := d.db.ExecContext(ctx, query, cutoffStr)
		if err != nil {
			return total, fmt.Errorf("deduplicator: purge: %w", err)
		}

		deleted, _ := result.RowsAffected()
		total += deleted

		if deleted == 0 {
			return total, nil
		}
	}
}

func (d *Deduplicator) purgeLoop(ctx context.Context) {
	defer d.wg.Done()

	d.runPurge(ctx)

	ticker := time.NewTicker(purgeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runPurge(ctx)
		}
	}
}

func (d *Deduplicator) runPurge(ctx context.Context) {
	cutoff := d.now().Add(-d.retention)

	// time.Now (monotonic) rather than d.now: measures wall-clock duration
	// independent of the injectable logical clock used for the cutoff.
	start := time.Now()
	deleted, err := d.purge(ctx, cutoff)
	elapsed := time.Since(start).Round(time.Microsecond)

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Printf("deduplicator: purge failed after %s: %v\n", elapsed, err)
		}

		return
	}

	log.Printf("deduplicator: purge completed in %s (removed %d domains older than %s)\n",
		elapsed, deleted, cutoff.UTC().Format(time.DateOnly))
}

func (d *Deduplicator) cacheAdd(domain string) {
	if d.cache != nil {
		d.cache.Add(domain, struct{}{})
	}
}

func (d *Deduplicator) Close() error {
	d.cancel()
	d.wg.Wait()

	if err := d.db.Close(); err != nil {
		return fmt.Errorf("deduplicator: close db: %w", err)
	}

	return nil
}
