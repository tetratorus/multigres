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
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

func TestRestoreSentinel_RoundTrip(t *testing.T) {
	pm := newTestManager(t)

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
	pm := newTestManager(t)
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

func TestRestoreFromBackupLocked_CleansUpFailedRestore(t *testing.T) {
	pm := newTestManager(t)
	pm.backup = backup.NewEngine(pm.logger, pm.runLongCommand, pm.record, backup.Settings{})

	dataDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "partial"), []byte("torn\n"), 0o644))

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	require.Error(t, pm.restoreFromBackupLocked(lockCtx, "missing-backup"))

	_, err = os.Stat(dataDir)
	assert.ErrorIs(t, err, os.ErrNotExist)
	present, err := pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.False(t, present)
}
