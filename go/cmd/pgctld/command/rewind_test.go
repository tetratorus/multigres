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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPgRewindSourceServer(t *testing.T) {
	conn, err := buildPgRewindSourceServer("pooler-1.zone1", 5432, "postgres", "zone1_pooler1")
	require.NoError(t, err)
	assert.Equal(t,
		"host='pooler-1.zone1' port=5432 user='postgres' dbname=postgres connect_timeout=5 require_auth=scram-sha-256 application_name='zone1_pooler1'",
		conn)

	conn, err = buildPgRewindSourceServer("::1", 5432, "postgres", "")
	require.NoError(t, err)
	assert.Equal(t, "host='::1' port=5432 user='postgres' dbname=postgres connect_timeout=5 require_auth=scram-sha-256", conn)

	conn, err = buildPgRewindSourceServer("127.0.0.1", 5432, `o'k\u`, "")
	require.NoError(t, err)
	assert.Contains(t, conn, `user='o\'k\\u'`)

	conn, err = buildPgRewindSourceServer("127.0.0.1", 5432, "postgres", "x'user=other")
	require.NoError(t, err)
	assert.Contains(t, conn, `application_name='x\'user=other'`)
}

func TestBuildPgRewindSourceServer_RejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		port    int32
		appName string
	}{
		{name: "empty host", host: "", port: 5432},
		{name: "host with space injection", host: "evil sslmode=disable", port: 5432},
		{name: "host with quote", host: "evil'", port: 5432},
		{name: "multi host list", host: "a,b", port: 5432},
		{name: "socket path", host: "/tmp", port: 5432},
		{name: "zero port", host: "localhost", port: 0},
		{name: "port too large", host: "localhost", port: 70000},
		{name: "app name keyword injection", host: "localhost", port: 5432, appName: "x user=other"},
		{name: "app name with control char", host: "localhost", port: 5432, appName: "x\nuser=other"},
		{name: "app name too long", host: "localhost", port: 5432, appName: strings.Repeat("a", 64)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildPgRewindSourceServer(tt.host, tt.port, "postgres", tt.appName)
			assert.Error(t, err)
		})
	}
}

func TestValidatePgRewindExtraArgs(t *testing.T) {
	require.NoError(t, validatePgRewindExtraArgs(nil))
	require.NoError(t, validatePgRewindExtraArgs([]string{"-R"}))
	require.NoError(t, validatePgRewindExtraArgs([]string{"--write-recovery-conf", "--progress"}))

	for _, bad := range [][]string{
		{"--source-server", "host=evil"},
		{"--source-server=host=evil"},
		{"--target-pgdata", "/tmp"},
		{"-c"},
		{"--restore-target-wal"},
		{"--config-file=/tmp/evil.conf"},
		{"-R", "-D", "/tmp"},
	} {
		assert.Error(t, validatePgRewindExtraArgs(bad), "args %v", bad)
	}
}
