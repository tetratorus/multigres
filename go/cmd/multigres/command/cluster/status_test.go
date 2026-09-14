// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func getStatusCommand() *cobra.Command {
	clusterCmd := &cobra.Command{Use: "cluster"}
	AddStatusCommand(clusterCmd)
	cmd, _, _ := clusterCmd.Find([]string{"status"})
	return cmd
}

func TestStatusCommandFlags(t *testing.T) {
	cmd := getStatusCommand()
	require.NotNil(t, cmd)

	tests := []struct {
		flag     string
		defValue string
	}{
		{"cell", ""},
		{"database", ""},
		{"output", "text"},
		{"timeout", (30 * time.Second).String()},
		{"admin-server", ""},
	}
	for _, tt := range tests {
		t.Run(tt.flag, func(t *testing.T) {
			f := cmd.Flag(tt.flag)
			require.NotNil(t, f, "flag %q should exist", tt.flag)
			assert.Equal(t, tt.defValue, f.DefValue)
		})
	}
}

func TestStatusCommandRejectsInvalidOutput(t *testing.T) {
	clusterCmd := &cobra.Command{Use: "cluster"}
	AddStatusCommand(clusterCmd)
	clusterCmd.SetOut(&bytes.Buffer{})
	clusterCmd.SetErr(&bytes.Buffer{})
	clusterCmd.SetArgs([]string{"status", "--output", "yaml", "--admin-server", "127.0.0.1:1"})

	err := clusterCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid --output "yaml"`)
}

// fakeStatusClient serves canned topology and per-pooler status responses.
type fakeStatusClient struct {
	cells    []string
	gateways []*clustermetadatapb.Multigateway
	// reachable lists gateway "cell/name" keys returned for only_reachable.
	reachable map[string]bool
	orchs     []*clustermetadatapb.Multiorch
	poolers   []*clustermetadatapb.Multipooler
	statuses  map[string]*multiadminpb.GetPoolerStatusResponse
	statusErr map[string]error

	gatewaysErr error
	orchsErr    error
	poolersErr  error

	poolerRequests []*multiadminpb.GetPoolersRequest
}

func (f *fakeStatusClient) GetCellNames(context.Context, *multiadminpb.GetCellNamesRequest, ...grpc.CallOption) (*multiadminpb.GetCellNamesResponse, error) {
	return &multiadminpb.GetCellNamesResponse{Names: f.cells}, nil
}

func (f *fakeStatusClient) GetGateways(_ context.Context, req *multiadminpb.GetGatewaysRequest, _ ...grpc.CallOption) (*multiadminpb.GetGatewaysResponse, error) {
	if f.gatewaysErr != nil {
		return nil, f.gatewaysErr
	}
	var out []*clustermetadatapb.Multigateway
	for _, gw := range f.gateways {
		if !inCells(req.Cells, gw.Id.Cell) {
			continue
		}
		if req.OnlyReachable && !f.reachable[idKey(gw.Id)] {
			continue
		}
		out = append(out, gw)
	}
	return &multiadminpb.GetGatewaysResponse{Gateways: out}, nil
}

func (f *fakeStatusClient) GetOrchs(_ context.Context, req *multiadminpb.GetOrchsRequest, _ ...grpc.CallOption) (*multiadminpb.GetOrchsResponse, error) {
	if f.orchsErr != nil {
		return nil, f.orchsErr
	}
	var out []*clustermetadatapb.Multiorch
	for _, o := range f.orchs {
		if inCells(req.Cells, o.Id.Cell) {
			out = append(out, o)
		}
	}
	return &multiadminpb.GetOrchsResponse{Orchs: out}, nil
}

func inCells(cells []string, cell string) bool {
	return len(cells) == 0 || slices.Contains(cells, cell)
}

func (f *fakeStatusClient) GetPoolers(_ context.Context, req *multiadminpb.GetPoolersRequest, _ ...grpc.CallOption) (*multiadminpb.GetPoolersResponse, error) {
	f.poolerRequests = append(f.poolerRequests, req)
	if f.poolersErr != nil {
		return nil, f.poolersErr
	}
	var out []*clustermetadatapb.Multipooler
	for _, p := range f.poolers {
		if req.Database != "" && p.ShardKey.Database != req.Database {
			continue
		}
		if !inCells(req.Cells, p.Id.Cell) {
			continue
		}
		out = append(out, p)
	}
	return &multiadminpb.GetPoolersResponse{Poolers: out}, nil
}

func (f *fakeStatusClient) GetPoolerStatus(_ context.Context, req *multiadminpb.GetPoolerStatusRequest, _ ...grpc.CallOption) (*multiadminpb.GetPoolerStatusResponse, error) {
	key := idKey(req.PoolerId)
	if err, ok := f.statusErr[key]; ok {
		return nil, err
	}
	resp, ok := f.statuses[key]
	if !ok {
		return nil, errors.New("no canned status for " + key)
	}
	return resp, nil
}

func pooler(cell, name, db, shard string, role clustermetadatapb.RoutingRole) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name},
		ShardKey:      &clustermetadatapb.ShardKey{Database: db, TableGroup: "default", Shard: shard},
		Hostname:      name + ".local",
		PortMap:       map[string]int32{"grpc": 15200, "postgres": 5432},
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		LifecycleStatus: &clustermetadatapb.PoolerLifecycle{
			Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		},
		RoutingState: &clustermetadatapb.RoutingState{Role: role},
	}
}

func primaryStatus(lsn string, followers ...*clustermetadatapb.ID) *multiadminpb.GetPoolerStatusResponse {
	return &multiadminpb.GetPoolerStatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PostgresRunning: true,
			PostgresReady:   true,
			PostgresStatus:  multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			WalPosition:     lsn,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:                lsn,
				Ready:              true,
				ConnectedFollowers: followers,
			},
		},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7},
						LeaderId:   &clustermetadatapb.ID{Cell: "zone1", Name: "pooler-a"},
					},
				},
			},
		},
	}
}

func standbyStatus(walReceiver string, lag time.Duration) *multiadminpb.GetPoolerStatusResponse {
	return &multiadminpb.GetPoolerStatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PostgresRunning: true,
			PostgresReady:   true,
			PostgresStatus:  multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY,
			WalPosition:     "0/2000000",
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReceiveLsn:    "0/2000000",
				LastReplayLsn:     "0/1FFFFF0",
				WalReceiverStatus: walReceiver,
				Lag:               durationpb.New(lag),
				PrimaryConnInfo:   &multipoolermanagerdatapb.PrimaryConnInfo{Host: "pooler-a.local", Port: 5432},
			},
		},
	}
}

func newFakeCluster() *fakeStatusClient {
	return &fakeStatusClient{
		cells: []string{"zone2", "zone1"},
		gateways: []*clustermetadatapb.Multigateway{
			{Id: &clustermetadatapb.ID{Cell: "zone1", Name: "gw-1"}, Hostname: "gw-1.local", PortMap: map[string]int32{"grpc": 15000, "postgres": 15432}},
			{Id: &clustermetadatapb.ID{Cell: "zone1", Name: "gw-stale"}, Hostname: "gw-stale.local", PortMap: map[string]int32{"grpc": 15000}},
		},
		reachable: map[string]bool{"zone1/gw-1": true},
		orchs: []*clustermetadatapb.Multiorch{
			{Id: &clustermetadatapb.ID{Cell: "zone1", Name: "orch-1"}, Hostname: "orch-1.local", PortMap: map[string]int32{"grpc": 15100}},
		},
		poolers: []*clustermetadatapb.Multipooler{
			pooler("zone1", "pooler-b", "postgres", "0", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA),
			pooler("zone1", "pooler-a", "postgres", "0", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY),
			pooler("zone2", "pooler-c", "postgres", "0", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA),
			pooler("zone2", "pooler-d", "postgres", "0", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA),
			pooler("zone1", "pooler-e", "analytics", "0", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA),
		},
		statuses: map[string]*multiadminpb.GetPoolerStatusResponse{
			"zone1/pooler-a": primaryStatus("0/2000000",
				&clustermetadatapb.ID{Cell: "zone1", Name: "pooler-b"},
				&clustermetadatapb.ID{Cell: "zone2", Name: "pooler-c"}),
			"zone1/pooler-b": standbyStatus("streaming", 15*time.Millisecond),
			"zone2/pooler-c": standbyStatus("", 0),
			"zone1/pooler-e": standbyStatus("streaming", 0),
		},
		statusErr: map[string]error{
			"zone2/pooler-d": errors.New("rpc error: code = Unavailable desc = connection refused"),
		},
	}
}

func TestCollectClusterStatus(t *testing.T) {
	client := newFakeCluster()
	report, err := collectClusterStatus(context.Background(), client, "", "")
	require.NoError(t, err)

	// Cells are sorted and carry gateways/orchs with probed reachability.
	require.Len(t, report.Cells, 2)
	assert.Equal(t, "zone1", report.Cells[0].Name)
	assert.Equal(t, "zone2", report.Cells[1].Name)
	require.Len(t, report.Cells[0].Gateways, 2)
	assert.Equal(t, "gw-1", report.Cells[0].Gateways[0].Name)
	assert.True(t, report.Cells[0].Gateways[0].Reachable)
	assert.Equal(t, "gw-1.local:15432", report.Cells[0].Gateways[0].PostgresAddress)
	assert.False(t, report.Cells[0].Gateways[1].Reachable)
	assert.Equal(t, 3, report.Cells[0].PoolerCount)
	assert.Equal(t, 2, report.Cells[1].PoolerCount)

	// Databases sorted, primary first within a shard.
	require.Len(t, report.Databases, 2)
	assert.Equal(t, "analytics", report.Databases[0].Name)
	assert.Equal(t, "postgres", report.Databases[1].Name)
	pg := report.Databases[1].Shards[0]
	assert.Equal(t, "zone1/pooler-a", pg.Primary)
	require.Len(t, pg.Poolers, 4)
	assert.Equal(t, "pooler-a", pg.Poolers[0].Name)
	assert.Equal(t, PoolerHealthy, pg.Poolers[0].Health)
	assert.Equal(t, int64(7), pg.Poolers[0].ConsensusTerm)
	assert.Equal(t, "zone1/pooler-a", pg.Poolers[0].ConsensusLeader)
	assert.Equal(t, []string{"zone1/pooler-b", "zone2/pooler-c"}, pg.Poolers[0].Primary.ConnectedFollowers)

	assert.Equal(t, "pooler-b", pg.Poolers[1].Name)
	assert.Equal(t, PoolerHealthy, pg.Poolers[1].Health)
	assert.Equal(t, "15ms", pg.Poolers[1].Replication.Lag)
	assert.Equal(t, "pooler-a.local:5432", pg.Poolers[1].Replication.UpstreamPrimary)

	assert.Equal(t, "pooler-c", pg.Poolers[2].Name)
	assert.Equal(t, PoolerDegraded, pg.Poolers[2].Health)
	assert.Equal(t, []string{"WAL receiver not running"}, pg.Poolers[2].Reasons)

	assert.Equal(t, "pooler-d", pg.Poolers[3].Name)
	assert.Equal(t, PoolerUnreachable, pg.Poolers[3].Health)
	assert.Contains(t, pg.Poolers[3].Error, "connection refused")

	// analytics has no primary.
	assert.Empty(t, report.Databases[0].Shards[0].Primary)
	assert.Equal(t, []string{"analytics/default/0"}, report.Summary.ShardsWithoutPrimary)

	assert.Equal(t, 5, report.Summary.Poolers)
	assert.Equal(t, 3, report.Summary.HealthyPoolers)
	assert.Equal(t, 2, report.Summary.Gateways)
	assert.Equal(t, 1, report.Summary.ReachableGateways)
	assert.Equal(t, 1, report.Summary.Orchestrators)
	assert.Empty(t, report.Warnings)
}

func TestCollectClusterStatusFilters(t *testing.T) {
	t.Run("cell filter skips GetCellNames and scopes every request", func(t *testing.T) {
		client := newFakeCluster()
		report, err := collectClusterStatus(context.Background(), client, "zone2", "")
		require.NoError(t, err)
		require.Len(t, report.Cells, 1)
		assert.Equal(t, "zone2", report.Cells[0].Name)
		require.Len(t, client.poolerRequests, 1)
		assert.Equal(t, []string{"zone2"}, client.poolerRequests[0].Cells)
		assert.Equal(t, 2, report.Summary.Poolers)
	})

	t.Run("database filter is passed through", func(t *testing.T) {
		client := newFakeCluster()
		report, err := collectClusterStatus(context.Background(), client, "", "analytics")
		require.NoError(t, err)
		assert.Equal(t, "analytics", client.poolerRequests[0].Database)
		require.Len(t, report.Databases, 1)
		assert.Equal(t, "analytics", report.Databases[0].Name)
	})
}

func TestCollectClusterStatusErrors(t *testing.T) {
	t.Run("pooler listing failure is fatal", func(t *testing.T) {
		client := newFakeCluster()
		client.poolersErr = errors.New("topo down")
		_, err := collectClusterStatus(context.Background(), client, "", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get poolers")
	})

	t.Run("gateway and orch failures degrade to warnings", func(t *testing.T) {
		client := newFakeCluster()
		client.gatewaysErr = errors.New("gw topo down")
		client.orchsErr = errors.New("orch topo down")
		report, err := collectClusterStatus(context.Background(), client, "", "")
		require.NoError(t, err)
		require.Len(t, report.Warnings, 2)
		assert.Contains(t, report.Warnings[0], "gw topo down")
		assert.Contains(t, report.Warnings[1], "orch topo down")
		assert.Equal(t, -1, report.Summary.ReachableGateways)
		assert.Equal(t, 5, report.Summary.Poolers)
	})
}

func TestDerivePoolerHealth(t *testing.T) {
	active := func(role clustermetadatapb.RoutingRole) *clustermetadatapb.Multipooler {
		return pooler("z", "p", "db", "0", role)
	}

	tests := []struct {
		name    string
		pooler  *clustermetadatapb.Multipooler
		status  *multipoolermanagerdatapb.Status
		health  PoolerHealth
		reasons []string
	}{
		{
			name:   "primary healthy",
			pooler: active(clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY),
			status: primaryStatus("0/1").Status,
			health: PoolerHealthy,
		},
		{
			name:   "routed primary but postgres still promoting",
			pooler: active(clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY),
			status: &multipoolermanagerdatapb.Status{
				PostgresRunning: true, PostgresReady: true,
				PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING,
			},
			health:  PoolerDegraded,
			reasons: []string{"routed as PRIMARY but postgres is POSTGRES_STATUS_PROMOTING"},
		},
		{
			name:    "postgres process dead",
			pooler:  active(clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA),
			status:  &multipoolermanagerdatapb.Status{PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_UNKNOWN},
			health:  PoolerDegraded,
			reasons: []string{"postgres not running"},
		},
		{
			name:   "postgres hung",
			pooler: active(clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA),
			status: &multipoolermanagerdatapb.Status{
				PostgresRunning: true,
				PostgresStatus:  multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY,
			},
			health:  PoolerDegraded,
			reasons: []string{"postgres running but not accepting connections"},
		},
		{
			name: "drained replica",
			pooler: func() *clustermetadatapb.Multipooler {
				p := active(clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
				p.ServingStatus = clustermetadatapb.PoolerServingStatus_DRAINING
				return p
			}(),
			status:  standbyStatus("streaming", 0).Status,
			health:  PoolerDegraded,
			reasons: []string{"not serving (DRAINING)"},
		},
		{
			name: "quarantined",
			pooler: func() *clustermetadatapb.Multipooler {
				p := active(clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
				p.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
					Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED,
					Reason: "pg_rewind failed",
				}
				return p
			}(),
			status:  standbyStatus("streaming", 0).Status,
			health:  PoolerDegraded,
			reasons: []string{"lifecycle LIFECYCLE_QUARANTINED: pg_rewind failed"},
		},
		{
			name:   "replica with paused replay",
			pooler: active(clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA),
			status: func() *multipoolermanagerdatapb.Status {
				s := standbyStatus("streaming", 0).Status
				s.ReplicationStatus.IsWalReplayPaused = true
				return s
			}(),
			health:  PoolerDegraded,
			reasons: []string{"WAL replay paused"},
		},
		{
			name:    "no routing role",
			pooler:  active(clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN),
			status:  standbyStatus("streaming", 0).Status,
			health:  PoolerDegraded,
			reasons: []string{"routing role unknown"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health, reasons := derivePoolerHealth(tt.pooler, tt.status)
			assert.Equal(t, tt.health, health)
			assert.Equal(t, tt.reasons, reasons)
		})
	}
}

func TestWriteText(t *testing.T) {
	report, err := collectClusterStatus(context.Background(), newFakeCluster(), "", "")
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, writeText(&buf, report))
	out := buf.String()

	assert.Contains(t, out, "Cells (2)")
	assert.Contains(t, out, "zone1: 2 gateway(s), 1 orchestrator(s), 3 pooler(s)")
	assert.Regexp(t, `gateway  gw-1\s+gw-1.local:15000\s+reachable  pg=gw-1.local:15432`, out)
	assert.Regexp(t, `gateway  gw-stale\s+gw-stale.local:15000\s+UNREACHABLE`, out)
	assert.Regexp(t, `orch     orch-1\s+orch-1.local:15100`, out)

	assert.Contains(t, out, "Database postgres")
	assert.Contains(t, out, "Shard default/0  primary=zone1/pooler-a")
	assert.Regexp(t, `PRIMARY  zone1/pooler-a\s+pooler-a.local:15200\s+HEALTHY\s+pg=PRIMARY\s+0/2000000`, out)
	assert.Contains(t, out, "followers=zone1/pooler-b,zone2/pooler-c")
	assert.Contains(t, out, "term=7 leader=zone1/pooler-a")
	assert.Regexp(t, `REPLICA  zone1/pooler-b\s+pooler-b.local:15200\s+HEALTHY\s+pg=STANDBY`, out)
	assert.Contains(t, out, "wal_receiver=streaming upstream=pooler-a.local:5432 lag=15ms")
	assert.Regexp(t, `REPLICA  zone2/pooler-c\s+pooler-c.local:15200\s+DEGRADED`, out)
	assert.Contains(t, out, "! WAL receiver not running")
	assert.Regexp(t, `REPLICA  zone2/pooler-d\s+pooler-d.local:15200\s+UNREACHABLE\s+pg=-`, out)
	assert.Contains(t, out, "! rpc error: code = Unavailable desc = connection refused")

	assert.Contains(t, out, "Shard default/0  primary=NONE")
	assert.Contains(t, out, "poolers:       3/5 healthy")
	assert.Contains(t, out, "gateways:      1/2 reachable")
	assert.Contains(t, out, "orchestrators: 1")
	assert.Contains(t, out, "WARNING: shard analytics/default/0 has no primary")
}

func TestWriteJSON(t *testing.T) {
	report, err := collectClusterStatus(context.Background(), newFakeCluster(), "", "")
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, writeJSON(&buf, report))

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
	summary := decoded["summary"].(map[string]any)
	assert.Equal(t, float64(5), summary["poolers"])
	assert.Equal(t, float64(3), summary["healthy_poolers"])
	assert.Equal(t, []any{"analytics/default/0"}, summary["shards_without_primary"])

	dbs := decoded["databases"].([]any)
	pg := dbs[1].(map[string]any)
	shard := pg["shards"].([]any)[0].(map[string]any)
	assert.Equal(t, "zone1/pooler-a", shard["primary"])
	first := shard["poolers"].([]any)[0].(map[string]any)
	assert.Equal(t, "ROUTING_ROLE_PRIMARY", first["routing_role"])
	assert.Equal(t, "POSTGRES_STATUS_PRIMARY", first["postgres_status"])
	assert.Equal(t, "HEALTHY", first["health"])
	_, hasReasons := first["reasons"]
	assert.False(t, hasReasons)

	// Round-trips into the exported type.
	var typed ClusterStatus
	require.NoError(t, json.Unmarshal(buf.Bytes(), &typed))
	assert.Equal(t, report.Summary, typed.Summary)
}
