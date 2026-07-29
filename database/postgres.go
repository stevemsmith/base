package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/alloydbconn"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/moov-io/base/log"
)

const (
	// PostgreSQL Error Codes
	// https://www.postgresql.org/docs/current/errcodes-appendix.html
	postgresErrUniqueViolation = "23505"
	postgresErrDeadlockFound   = "40P01"

	// Bound ShouldPing wait so acquire retries during failover stay inside
	// typical request budgets. AlloyDB disconnects usually fail fast (TCP RST);
	// this caps hung/TIME_WAIT peers.
	defaultPostgresPingTimeout = time.Second
)

func postgresConnection(ctx context.Context, logger log.Logger, config PostgresConfig, databaseName string) (*sql.DB, error) {
	poolConfig, dialer, err := buildPgxPoolConfig(ctx, config, databaseName)
	if err != nil {
		return nil, logger.LogErrorf("building pgx pool config: %w", err).Err()
	}

	// Apply connection limits to pgxpool (not database/sql). OpenDBFromPool
	// requires sql.DB MaxIdleConns=0; sql.DB setters do not configure the
	// underlying pool and SetMaxIdleConns(n>0) actively breaks it.
	ApplyPostgresPoolConfig(logger, poolConfig, config.Connections)

	// HealthCheckPeriod is the background reaper cadence. It only evicts
	// connections that exceed MaxConnLifetime / MaxConnIdleTime — it does not
	// ping. Liveness at acquire time is handled by pgxpool's default
	// ShouldPing (idle > 1s) unless callers customize it later.
	poolConfig.HealthCheckPeriod = 1 * time.Second

	if poolConfig.PingTimeout <= 0 {
		poolConfig.PingTimeout = defaultPostgresPingTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		_ = closeAlloyDialer(dialer)
		return nil, logger.LogErrorf("creating pgx pool: %w", err).Err()
	}

	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()
		_ = closeAlloyDialer(dialer)
		return nil, logger.LogErrorf("connecting to database: %w", err).Err()
	}

	// OpenDBFromPool does not close the pool when *sql.DB is closed. Wrap the
	// connector so db.Close() shuts down the pool (and AlloyDB dialer).
	db := openDBFromPool(pool, dialer)

	return db, nil
}

// ApplyPostgresPoolConfig fills zero-valued fields in connections with
// DefaultPostgresConnectionsConfig, then maps them onto poolConfig.
//
// Unlike database/sql (where MaxOpen=0 means unlimited), pgxpool always has a
// finite MaxConns. Leaving MaxOpen unset previously fell through to pgxpool's
// default of max(4, NumCPU()), which silently shrinks pools for services that
// never configured Connections. We instead apply explicit library defaults so
// behavior is predictable and logged.
//
// MaxIdle has no pgxpool "max idle" equivalent. When set (or defaulted), it is
// applied as MinIdleConns (warm floor), capped by MaxConns, so the field still
// influences pool shape rather than being dropped on the floor.
func ApplyPostgresPoolConfig(logger log.Logger, poolConfig *pgxpool.Config, connections ConnectionsConfig) {
	if poolConfig == nil {
		return
	}

	applied := ResolvePostgresConnectionsConfig(connections)

	logger.Logf("setting pgx pool MaxConns to %d", applied.MaxOpen)
	poolConfig.MaxConns = int32(applied.MaxOpen)

	minIdle := applied.MaxIdle
	if minIdle > applied.MaxOpen {
		minIdle = applied.MaxOpen
	}
	if minIdle < 0 {
		minIdle = 0
	}
	logger.Logf("setting pgx pool MinIdleConns to %d (from ConnectionsConfig.MaxIdle)", minIdle)
	poolConfig.MinIdleConns = int32(minIdle)

	logger.Logf("setting pgx pool MaxConnIdleTime to %v", applied.MaxIdleTime)
	poolConfig.MaxConnIdleTime = applied.MaxIdleTime

	logger.Logf("setting pgx pool MaxConnLifetime to %v", applied.MaxLifetime)
	poolConfig.MaxConnLifetime = applied.MaxLifetime
}

// ResolvePostgresConnectionsConfig returns connections with zero-valued fields
// replaced by DefaultPostgresConnectionsConfig.
func ResolvePostgresConnectionsConfig(connections ConnectionsConfig) ConnectionsConfig {
	defaults := DefaultPostgresConnectionsConfig()
	if connections.MaxOpen <= 0 {
		connections.MaxOpen = defaults.MaxOpen
	}
	if connections.MaxIdle <= 0 {
		connections.MaxIdle = defaults.MaxIdle
	}
	if connections.MaxLifetime <= 0 {
		connections.MaxLifetime = defaults.MaxLifetime
	}
	if connections.MaxIdleTime <= 0 {
		connections.MaxIdleTime = defaults.MaxIdleTime
	}
	return connections
}

// openDBFromPool wraps pgxpool in a *sql.DB whose Close also closes the pool
// and optional AlloyDB dialer. stdlib.OpenDBFromPool alone leaks both.
func openDBFromPool(pool *pgxpool.Pool, dialer *alloydbconn.Dialer) *sql.DB {
	c := &poolConnector{
		Connector: stdlib.GetPoolConnector(pool),
		pool:      pool,
		dialer:    dialer,
	}
	db := sql.OpenDB(c)
	// Required when using a pgxpool-backed connector: non-zero idle conns on
	// sql.DB prevent connections from being released back to the pool.
	db.SetMaxIdleConns(0)
	return db
}

// poolConnector delegates to pgx stdlib's pool connector and implements
// io.Closer so database/sql.DB.Close shuts down the underlying pgxpool.
type poolConnector struct {
	driver.Connector
	pool   *pgxpool.Pool
	dialer *alloydbconn.Dialer

	closeOnce sync.Once
	closeErr  error
}

func (c *poolConnector) Close() error {
	c.closeOnce.Do(func() {
		if c.pool != nil {
			c.pool.Close()
		}
		c.closeErr = closeAlloyDialer(c.dialer)
	})
	return c.closeErr
}

func closeAlloyDialer(dialer *alloydbconn.Dialer) error {
	if dialer == nil {
		return nil
	}
	return dialer.Close()
}

func buildPgxPoolConfig(ctx context.Context, config PostgresConfig, databaseName string) (*pgxpool.Config, *alloydbconn.Dialer, error) {
	if config.Alloy != nil {
		return buildAlloyDBPoolConfig(ctx, config, databaseName)
	}

	connStr, err := getPostgresConnStr(config, databaseName)
	if err != nil {
		return nil, nil, err
	}
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, nil, err
	}
	return poolConfig, nil, nil
}

func buildAlloyDBPoolConfig(ctx context.Context, config PostgresConfig, databaseName string) (*pgxpool.Config, *alloydbconn.Dialer, error) {
	if config.Alloy == nil {
		return nil, nil, fmt.Errorf("missing alloy config")
	}

	var dialer *alloydbconn.Dialer
	var dsn string

	if config.Alloy.UseIAM {
		d, err := alloydbconn.NewDialer(ctx, alloydbconn.WithIAMAuthN())
		if err != nil {
			return nil, nil, fmt.Errorf("creating alloydb dialer: %w", err)
		}
		dialer = d
		dsn = fmt.Sprintf(
			// sslmode is disabled because the alloy db connection dialer will handle it
			// no password is used with IAM
			"user=%s dbname=%s sslmode=disable",
			config.User, databaseName,
		)
	} else {
		d, err := alloydbconn.NewDialer(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("creating alloydb dialer: %w", err)
		}
		dialer = d
		dsn = fmt.Sprintf(
			// sslmode is disabled because the alloy db connection dialer will handle it
			"user=%s password=%s dbname=%s sslmode=disable",
			config.User, config.Password, databaseName,
		)
	}

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_ = closeAlloyDialer(dialer)
		return nil, nil, fmt.Errorf("failed to parse pgx pool config: %w", err)
	}

	var connOptions []alloydbconn.DialOption
	if config.Alloy.UsePSC {
		connOptions = append(connOptions, alloydbconn.WithPSC())
	}

	poolConfig.ConnConfig.DialFunc = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		return dialer.Dial(ctx, config.Alloy.InstanceURI, connOptions...)
	}

	return poolConfig, dialer, nil
}

func getPostgresConnStr(config PostgresConfig, databaseName string) (string, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s/%s", config.User, config.Password, config.Address, databaseName)

	params := ""

	if config.TLS != nil {
		if len(config.TLS.Mode) < 1 {
			config.TLS.Mode = "verify-full"
		}

		params += "sslmode=" + config.TLS.Mode

		if len(config.TLS.CACertFile) > 0 {
			params += "&sslrootcert=" + config.TLS.CACertFile
		}

		if len(config.TLS.ClientCertFile) > 0 {
			params += "&sslcert=" + config.TLS.ClientCertFile
		}

		if len(config.TLS.ClientKeyFile) > 0 {
			params += "&sslkey=" + config.TLS.ClientKeyFile
		}
	}

	connStr := fmt.Sprintf("%s?%s", url, params)
	return connStr, nil
}

// PostgresUniqueViolation returns true when the provided error matches the Postgres code
// for unique violation.
func PostgresUniqueViolation(err error) bool {
	if err == nil {
		return false
	}

	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError.Code == postgresErrUniqueViolation {
		return true
	}

	return strings.Contains(err.Error(), postgresErrUniqueViolation)
}

// PostgresDeadlockFound returns true when the provided error matches the Postgres code
// for deadlock found.
func PostgresDeadlockFound(err error) bool {
	if err == nil {
		return false
	}

	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError.Code == postgresErrDeadlockFound {
		return true
	}

	return strings.Contains(err.Error(), postgresErrDeadlockFound)
}

// IsRetryablePostgresError returns true if the error is a transient connection-level
// error that is safe to retry. This covers the errors seen during AlloyDB maintenance
// switchovers and other transient network failures.
func IsRetryablePostgresError(err error) bool {
	if err == nil {
		return false
	}

	// PostgreSQL error codes indicating the server is shutting down or unavailable
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P01", "57P02", "57P03": // admin_shutdown, crash_shutdown, cannot_connect_now
			return true
		case "08000", "08001", "08003", "08004", "08006": // connection_exception class
			return true
		}
		return false
	}

	// Network-level errors: connection reset, broken pipe, EOF, etc.
	// These occur when the TCP connection is severed during a switchover.
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false // don't retry if the caller's context timed out
	}

	// pgx wraps connection errors with these messages
	msg := err.Error()
	if strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "unexpected EOF") ||
		strings.Contains(msg, "conn closed") {
		return true
	}

	return false
}

// RetryPostgres executes fn up to maxAttempts times, retrying on transient
// connection errors. This is intended for use around individual database
// operations to survive brief outages like AlloyDB maintenance switchovers.
func RetryPostgres(ctx context.Context, maxAttempts int, fn func() error) error {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !IsRetryablePostgresError(err) {
			return err
		}
		if attempt < maxAttempts-1 {
			backoff := time.Duration(attempt+1) * 200 * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return err
}
