// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/executil"
)

type PgRewindResult struct {
	// Status message
	Message string
	Output  string
}

// maxPgRewindApplicationNameLength is PostgreSQL's application_name limit (NAMEDATALEN - 1).
const maxPgRewindApplicationNameLength = 63

// allowedPgRewindExtraArgs lists the pg_rewind flags a caller may add. Anything
// else (e.g. --source-server, --target-pgdata, --restore-target-wal,
// --config-file) could redirect pg_rewind or make it run arbitrary commands.
var allowedPgRewindExtraArgs = map[string]bool{
	"-R":                    true,
	"--write-recovery-conf": true,
	"-P":                    true,
	"--progress":            true,
	"--debug":               true,
}

var pgRewindHostnameRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,251}[A-Za-z0-9])?$`)

// validatePgRewindExtraArgs rejects any pg_rewind argument not in allowedPgRewindExtraArgs.
func validatePgRewindExtraArgs(extraArgs []string) error {
	for _, arg := range extraArgs {
		if !allowedPgRewindExtraArgs[arg] {
			return fmt.Errorf("pg_rewind argument %q is not allowed", arg)
		}
	}
	return nil
}

// quoteConnValue quotes a value for a libpq keyword/value connection string so
// it cannot add or override other connection parameters.
func quoteConnValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}

// buildPgRewindSourceServer validates the caller-supplied source fields and
// returns the libpq connection string pg_rewind uses to reach the source.
//
// The password is not part of the string (it is passed via PGPASSWORD), but it
// is sent to whatever host is named here. require_auth=scram-sha-256 stops
// libpq from handing the password over in cleartext (or under md5) to a host
// that asks for it; every multigres pg_hba.conf entry uses scram-sha-256.
func buildPgRewindSourceServer(host string, port int32, user, applicationName string) (string, error) {
	if host == "" {
		return "", errors.New("source_host is required")
	}
	if net.ParseIP(host) == nil && !pgRewindHostnameRe.MatchString(host) {
		return "", fmt.Errorf("invalid source_host %q", host)
	}
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("invalid source_port %d", port)
	}
	if len(applicationName) > maxPgRewindApplicationNameLength {
		return "", fmt.Errorf("application_name exceeds %d characters", maxPgRewindApplicationNameLength)
	}
	for _, r := range applicationName {
		if r < 0x21 || r > 0x7e {
			return "", fmt.Errorf("invalid character in application_name %q", applicationName)
		}
	}

	// connect_timeout ensures pg_rewind fails fast if the source is not yet ready to accept
	// connections (e.g. newly-promoted primary still in crash recovery), allowing the caller
	// to retry rather than blocking for the OS TCP timeout.
	conn := fmt.Sprintf("host=%s port=%d user=%s dbname=postgres connect_timeout=5 require_auth=scram-sha-256",
		quoteConnValue(host), port, quoteConnValue(user))
	if applicationName != "" {
		conn += " application_name=" + quoteConnValue(applicationName)
	}
	return conn, nil
}

func PgRewindWithResult(ctx context.Context, logger *slog.Logger, sourceServer, password string, dryRun bool, extraArgs []string) (*PgRewindResult, error) {
	result := &PgRewindResult{}
	if err := validatePgRewindExtraArgs(extraArgs); err != nil {
		result.Message = "Rewind failed"
		return result, err
	}
	dataDir := pgctld.PostgresDataDir()

	args := []string{
		"--source-server", sourceServer,
		"--target-pgdata", dataDir,
	}
	if dryRun {
		args = append(args, "--dry-run")
	}
	args = append(args, extraArgs...)

	logger.InfoContext(ctx, "executing pg_rewind command",
		"command", "pg_rewind",
		"args", args,
		"source_server", sourceServer,
		"target_pgdata", dataDir,
		"dry_run", dryRun)

	cmd := executil.Command(ctx, "pg_rewind", args...)

	// Set PGPASSWORD environment variable for pg_rewind to use
	// pg_rewind doesn't reliably use passwords from connection strings
	if password != "" {
		cmd.AddEnv("PGPASSWORD=" + password)
	}

	// Capture both Stdout and Stderr. Time the subprocess so the raw pg_rewind
	// runtime is visible in pgctld logs (the multipooler records it as the
	// multipooler.rewind.execution.duration metric); pg_rewind runtime scales with
	// retained pg_wal and can run for minutes, so it is worth surfacing.
	start := time.Now()
	output, err := cmd.CombinedOutput()
	duration := time.Since(start)
	result.Output = string(output)
	if err != nil {
		result.Message = "Rewind failed"
		logger.ErrorContext(ctx, "pg_rewind command failed",
			"error", err,
			"duration", duration.String(),
			"dry_run", dryRun,
			"output", string(output))
		return result, fmt.Errorf("pg_rewind failed: %w", err)
	}

	result.Message = "Rewind completed successfully"
	logger.InfoContext(ctx, "pg_rewind command completed successfully",
		"duration", duration.String(),
		"dry_run", dryRun,
		"output", string(output))
	return result, nil
}
