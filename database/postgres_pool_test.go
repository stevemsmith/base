package database

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestOpenDBFromPool_CloseClosesPool(t *testing.T) {
	// NewWithConfig does not dial until a connection is needed; use an
	// unreachable address so this stays a pure unit test.
	cfg, err := pgxpool.ParseConfig("postgres://user:pass@127.0.0.1:1/db?sslmode=disable")
	require.NoError(t, err)
	cfg.MaxConns = 2

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)

	db := openDBFromPool(pool, nil)
	require.NoError(t, db.Close())

	// Closing the sql.DB must close the underlying pgxpool.
	_, err = pool.Acquire(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "closed")
}

func TestOpenDBFromPool_CloseIdempotent(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://user:pass@127.0.0.1:1/db?sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)

	db := openDBFromPool(pool, nil)
	require.NoError(t, db.Close())
	require.NoError(t, db.Close())
}
