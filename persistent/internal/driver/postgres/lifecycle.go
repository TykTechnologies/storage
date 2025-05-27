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
	connStr := opts.ConnectionString
	if opts.UseSSL {
		connStr += " sslmode=require"
		if opts.SSLInsecureSkipVerify {
			connStr += " sslmode=require sslrootcert=verify-ca"
		}
		if opts.SSLCAFile != "" {
			connStr += fmt.Sprintf(" sslrootcert=%s", opts.SSLCAFile)
		}
		if opts.SSLPEMKeyfile != "" {
			connStr += fmt.Sprintf(" sslcert=%s", opts.SSLPEMKeyfile)
		}
	} else {
		connStr += " sslmode=disable"
	}

	var err error
	l.db, err = gorm.Open(postgres.Open(connStr), &gorm.Config{})
	if err != nil {
		return err
	}

	l.sqlDB, err = l.db.DB()
	if err != nil {
		return err
	}

	// Set connection timeout
	if opts.ConnectionTimeout > 0 {
		l.sqlDB.SetConnMaxLifetime(time.Duration(opts.ConnectionTimeout) * time.Second)
	}

	// Extract database name from connection string
	parts := strings.Split(connStr, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "dbname=") {
			l.dbName = strings.TrimPrefix(part, "dbname=")
			break
		}
	}

	return l.sqlDB.Ping()
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

func (p *driver) Ping(ctx context.Context) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	// Get the underlying *sql.DB from GORM
	sqlDB, err := p.db.DB()
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

func (p *driver) DropDatabase(_ context.Context) error {
	// Check if the database connection is valid
	if p.db == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	dbNameToDelete := p.dbName

	// In PostgresSQL, we cannot drop the currently connected database.
	// We need to provide instructions on how to drop the database manually.

	// Close the current connection
	err := p.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}

	// Set the driver's db to nil to prevent further use
	p.db = nil

	// Return a special error with instructions
	return fmt.Errorf(
		"postgreSQL does not allow dropping the currently connected database. "+
			"To drop the database '%s', connect to another database (like 'postgres') "+
			"and execute: DROP DATABASE %s; ",
		dbNameToDelete, dbNameToDelete)
}

var _ types.StorageLifecycle = &lifeCycle{}
