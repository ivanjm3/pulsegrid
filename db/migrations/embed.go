// Package migrations embeds the SQL migration files applied to Postgres at
// API server startup (task 25).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
