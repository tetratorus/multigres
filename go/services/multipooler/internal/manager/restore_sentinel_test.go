// Copyright 2026 Supabase, Inc.
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

package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

func TestRestoreSentinel_RoundTrip(t *testing.T) {
	pm := newSentinelTestManager(t)

	present, err := pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.False(t, present)

	require.NoError(t, pm.writeRestoreSentinel())
	present, err = pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.True(t, present)

	require.NoError(t, pm.removeRestoreSentinel())
	present, err = pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.False(t, present)
	require.NoError(t, pm.removeRestoreSentinel())
}

func TestRestoreAndStartPostgres_RemovesTornDataDirectory(t *testing.T) {
	pm := newSentinelTestManager(t)
	pm.pgctldClient = &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
	}
	pm.backup = backup.NewEngine(pm.logger, pm.runLongCommand, pm.record, backup.Settings{})

	dataDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("16\n"), 0o644))
	require.NoError(t, pm.writeRestoreSentinel())

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	require.Error(t, pm.restoreAndStartPostgres(lockCtx))
	_, err = os.Stat(dataDir)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestRestoreAndStartPostgres_PreservesDataDirectoryWhileRunning(t *testing.T) {
	pm := newSentinelTestManager(t)
	pm.pgctldClient = &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_RUNNING},
	}
	pm.backup = backup.NewEngine(pm.logger, pm.runLongCommand, pm.record, backup.Settings{})

	dataDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
	pgVersionPath := filepath.Join(dataDir, "PG_VERSION")
	require.NoError(t, os.WriteFile(pgVersionPath, []byte("16\n"), 0o644))
	require.NoError(t, pm.writeRestoreSentinel())

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	require.NoError(t, pm.restoreAndStartPostgres(lockCtx))
	assert.FileExists(t, pgVersionPath)
	present, err := pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.True(t, present)
}

func TestRestoreFromBackupLocked_CleansUpFailedRestore(t *testing.T) {
	pm := newSentinelTestManager(t)
	pm.backup = backup.NewEngine(pm.logger, pm.runLongCommand, pm.record, backup.Settings{})

	dataDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "partial"), []byte("torn\n"), 0o644))

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	err = pm.restoreFromBackupLocked(lockCtx, "missing-backup")
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	assert.ErrorContains(t, err, "PGDATA is not empty")

	assert.FileExists(t, filepath.Join(dataDir, "partial"))
	present, err := pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.False(t, present)
}

func TestRestoreFromBackupLocked_CleansUpFailedRestoreWithEmptyDataDirectory(t *testing.T) {
	pm := newSentinelTestManager(t)
	pm.backup = backup.NewEngine(pm.logger, pm.runLongCommand, pm.record, backup.Settings{})

	dataDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, dataDir)

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	require.Error(t, pm.restoreFromBackupLocked(lockCtx, "missing-backup"))

	entries, err := os.ReadDir(dataDir)
	if err == nil {
		assert.Empty(t, entries)
	} else {
		assert.ErrorIs(t, err, os.ErrNotExist)
	}
	present, err := pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.False(t, present)
}
