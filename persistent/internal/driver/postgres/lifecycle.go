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
	writeDB *gorm.DB
	readDB  *gorm.DB

	dbName     string
	writeSQLDB *sql.DB
	readSQLDB  *sql.DB
}

func standardizePgDSN(dsn string) (string, error) {
	// Use the connection string exactly as provided
	connString := strings.TrimSpace(dsn)
	if dsn == "" {
		return "", errors.New("empty connection string")
	}

	return connString, nil
}

func (l *lifeCycle) establishConnection(connStr string, opts *types.ClientOpts, isWrite bool) error {
	dsn, err := standardizePgDSN(connStr)
	if err != nil {
		return err
	}

	// Add SSL parameters
	dsn = l.addSSLParams(dsn, opts)

	// Open connection
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("gorm open: %w", err)
	}

	// Get underlying SQL DB
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("getting sql.DB: %w", err)
	}

	// Configure connection
	if opts.ConnectionTimeout > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(opts.ConnectionTimeout) * time.Second)
	}

	// Ping to verify connection
	pingTimeout := 5 * time.Second
	if opts.ConnectionTimeout > 0 {
		pingTimeout = time.Duration(opts.ConnectionTimeout) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	// Store connection based on type
	if isWrite {
		l.writeDB = db
		l.writeSQLDB = sqlDB

		// Extract database name (only needed once)
		l.extractDBName(dsn)
	} else {
		l.readDB = db
		l.readSQLDB = sqlDB
	}

	return nil
}

// Connect initializes a new database connection using the provided client options.
// Returns an error if the connection cannot be established.
func (l *lifeCycle) Connect(opts *types.ClientOpts) error {
	if opts == nil {
		return errors.New("nil opts")
	}

	// Establish write connection
	if err := l.establishConnection(opts.ConnectionString, opts, true); err != nil {
		return fmt.Errorf("write connection failed: %w", err)
	}

	// Determine read connection string
	readConnStr := opts.ReadConnectionString
	if readConnStr == "" {
		// If no separate read connection, use write connection for reads
		l.readDB = l.writeDB
		l.readSQLDB = l.writeSQLDB
	} else {
		// Establish separate read connection
		if readErr := l.establishConnection(readConnStr, opts, false); readErr != nil {

			// Try to close the write connection, but don't lose the original error
			if closeErr := l.closeWriteConnection(); closeErr != nil {
				// Combine both errors in the message
				return fmt.Errorf("read connection failed: %w; additionally, failed to close write connection: %v", readErr, closeErr)
			}

			// If write connection closed successfully, return the original error
			return fmt.Errorf("read connection failed: %w", readErr)
		}
	}

	return nil
}

func (l *lifeCycle) closeWriteConnection() error {
	if l.writeSQLDB != nil {
		err := l.writeSQLDB.Close()
		if err != nil {
			return err
		}
		l.writeDB = nil
		l.writeSQLDB = nil
	}
	return nil
}

func (l *lifeCycle) extractDBName(dsn string) {
	if parts := strings.Fields(dsn); len(parts) > 0 {
		for _, part := range parts {
			if strings.HasPrefix(part, "dbname=") {
				l.dbName = strings.TrimPrefix(part, "dbname=")
				break
			}
		}
	}
	if l.dbName == "" && (strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")) {
		if u, err := url.Parse(dsn); err == nil {
			l.dbName = strings.TrimPrefix(u.Path, "/")
		}
	}
}

// Helper to add SSL parameters
func (l *lifeCycle) addSSLParams(dsn string, opts *types.ClientOpts) string {
	params := []string{}

	if opts.UseSSL {
		mode := "verify-full"
		if opts.SSLInsecureSkipVerify {
			mode = "require"
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
		if dsn != "" {
			dsn = dsn + " " + strings.Join(params, " ")
		} else {
			dsn = strings.Join(params, " ")
		}
	}

	return dsn
}

// Close terminates the active database connection.
// Returns an error if the connection cannot be closed properly.
func (l *lifeCycle) Close() error {
	var writeErr, readErr error

	// Close write connection
	if l.writeSQLDB != nil {
		writeErr = l.writeSQLDB.Close()
		l.writeDB = nil
		l.writeSQLDB = nil
	}

	// Close read connection (only if different from write)
	if l.readSQLDB != nil && l.readSQLDB != l.writeSQLDB {
		readErr = l.readSQLDB.Close()
		l.readDB = nil
		l.readSQLDB = nil
	}

	if writeErr != nil {
		return writeErr
	}
	return readErr
}

// DBType returns the type of database managed by this lifecycle.
// Useful for distinguishing between supported database backends.
func (l *lifeCycle) DBType() utils.DBType {
	return utils.PostgresDB
}

// Ping checks the health of the database connection.
// Returns an error if the database is unreachable or not responding.
func (d *driver) Ping(ctx context.Context) error {
	// Check if connections exist
	if d.writeDB == nil || d.readDB == nil {
		return errors.New(types.ErrorSessionClosed)
	}

	if ctx == nil {
		return errors.New(types.ErrorNilContext)
	}

	// Ping write connection
	writeSQLDB, err := d.writeDB.DB()
	if err != nil {
		return fmt.Errorf("failed to get write database connection: %w", err)
	}

	if err := writeSQLDB.PingContext(ctx); err != nil {
		return fmt.Errorf("write database ping failed: %w", err)
	}

	// If read and write are the same connection, we're done
	if d.readDB == d.writeDB {
		return nil
	}

	// Ping read connection
	readSQLDB, err := d.readDB.DB()
	if err != nil {
		return fmt.Errorf("failed to get read database connection: %w", err)
	}

	if err := readSQLDB.PingContext(ctx); err != nil {
		return fmt.Errorf("read database ping failed: %w", err)
	}

	return nil
}

// DropDatabase removes the connected database entirely.
// Returns an error if the operation fails.
func (d *driver) DropDatabase(_ context.Context) error {
	// Check if the database connection is valid
	if d.writeDB == nil {
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

	// Set the driver's writeDB to nil to prevent further use
	d.writeDB = nil

	// Return a special error with instructions
	return fmt.Errorf(
		"postgreSQL does not allow dropping the currently connected database. "+
			"To drop the database '%s', connect to another database (like 'postgres') "+
			"and execute: DROP DATABASE %s; ",
		dbNameToDelete, dbNameToDelete)
}

var _ types.StorageLifecycle = &lifeCycle{}
