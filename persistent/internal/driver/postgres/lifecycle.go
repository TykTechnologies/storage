package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/utils"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"net/url"
	"strings"
	"time"
)

type lifeCycle struct {
	db               *gorm.DB
	connectionString string
	dbName           string
	sqlDB            *sql.DB
}

func (l *lifeCycle) Connect(opts *types.ClientOpts) error {
	if opts == nil {
		return errors.New("nil opts")
	}
	// Start with whatever user provided (could be DSN or connection URL)
	connStr := strings.TrimSpace(opts.ConnectionString)

	// Build parameter list to append (keeps original string intact)
	params := []string{}

	if opts.UseSSL {
		// Decide sslmode
		mode := "verify-full" // safest default
		if opts.SSLInsecureSkipVerify {
			mode = "require" // encrypted but no verification
		}

		params = append(params, fmt.Sprintf("sslmode=%s", mode))

		if opts.SSLCAFile != "" {
			params = append(params, fmt.Sprintf("sslrootcert=%s", opts.SSLCAFile))
		}
		if opts.SSLPEMKeyfile != "" {
			params = append(params, fmt.Sprintf("sslcert=%s", opts.SSLPEMKeyfile))
		}
	} else {
		params = append(params, "sslmode=disable")
	}

	if len(params) > 0 {
		if connStr != "" {
			connStr = connStr + " " + strings.Join(params, " ")
		} else {
			connStr = strings.Join(params, " ")
		}
	}

	var err error
	db, err := gorm.Open(postgres.Open(connStr), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("gorm open: %w", err)
	}
	l.db = db

	sqlDB, err := db.DB()
	if err != nil {
		l.db = nil
		return fmt.Errorf("getting sql.DB: %w", err)
	}
	l.sqlDB = sqlDB

	// Set connection timeout
	if opts.ConnectionTimeout > 0 {
		l.sqlDB.SetConnMaxLifetime(time.Duration(opts.ConnectionTimeout) * time.Second)
	}

	pingTimeout := 5 * time.Second
	if opts.ConnectionTimeout > 0 {
		pingTimeout = time.Duration(opts.ConnectionTimeout) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := l.sqlDB.PingContext(ctx); err != nil {
		l.db = nil
		return fmt.Errorf("ping db: %w", err)
	}

	// Extract database name. Handle both DSN form (dbname=...) and URL form (postgres://host/dbname)
	// DSN search
	for _, part := range strings.Fields(connStr) {
		if strings.HasPrefix(part, "dbname=") {
			l.dbName = strings.TrimPrefix(part, "dbname=")
			break
		}
	}

	// If not found, try parsing URL
	if l.dbName == "" {
		if strings.HasPrefix(opts.ConnectionString, "postgres://") || strings.HasPrefix(opts.ConnectionString, "postgresql://") {
			if u, err := url.Parse(opts.ConnectionString); err == nil {
				l.dbName = strings.TrimPrefix(u.Path, "/")
			}
		}
	}

	return nil
}

func (l *lifeCycle) Close() error {
	err := l.sqlDB.Close()
	if err != nil {
		return err
	}
	l.db = nil
	return nil
}

func (l *lifeCycle) DBType() utils.DBType {
	return utils.PostgresDB
}

func (d *driver) Ping(ctx context.Context) error {
	// Check if the database connection is valid
	if d.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	if ctx == nil {
		return errors.New(types.ErrorNilContext)
	}

	// Get the underlying *sql.DB from GORM
	sqlDB, err := d.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	// Use the standard library's PingContext method
	err = sqlDB.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	return nil
}

func (d *driver) DropDatabase(_ context.Context) error {
	// Check if the database connection is valid
	if d.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	dbNameToDelete := d.dbName

	// In PostgresSQL, we cannot drop the currently connected database.
	// We need to provide instructions on how to drop the database manually.

	// Close the current connection
	err := d.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	// Set the driver's db to nil to prevent further use
	d.db = nil

	// Return a special error with instructions
	return fmt.Errorf(
		"postgreSQL does not allow dropping the currently connected database. "+
			"To drop the database '%s', connect to another database (like 'postgres') "+
			"and execute: DROP DATABASE %s; ",
		dbNameToDelete, dbNameToDelete)
}

var _ types.StorageLifecycle = &lifeCycle{}
