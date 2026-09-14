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

	"github.com/multigres/multigres/go/common/constants"
)

// restoreSentinelPath is the on-disk location of the restore sentinel. It
// lives in pooler_dir (not PGDATA) so it is not captured by pgBackRest backups.
func (pm *MultipoolerManager) restoreSentinelPath() string {
	return filepath.Join(pm.record.PoolerDir(), constants.RestoreSentinelFile)
}

// hasRestoreSentinel reports whether the restore sentinel exists. A
// non-existent file is (false, nil); any other stat failure is surfaced.
func (pm *MultipoolerManager) hasRestoreSentinel() (bool, error) {
	_, err := os.Stat(pm.restoreSentinelPath())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// writeRestoreSentinel creates the restore sentinel and makes it durable.
func (pm *MultipoolerManager) writeRestoreSentinel() error {
	path := pm.restoreSentinelPath()
	if err := os.WriteFile(path, []byte("restore in progress\n"), 0o644); err != nil {
		return err
	}
	if err := fsyncPath(path); err != nil {
		return err
	}
	return fsyncPath(filepath.Dir(path))
}

// removeRestoreSentinel deletes the restore sentinel; a missing file is not an
// error.
func (pm *MultipoolerManager) removeRestoreSentinel() error {
	if err := os.Remove(pm.restoreSentinelPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
