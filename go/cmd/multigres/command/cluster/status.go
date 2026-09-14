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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/viperutil"
)

const (
	outputText = "text"
	outputJSON = "json"
)

type statusCmd struct {
	cell     viperutil.Value[string]
	database viperutil.Value[string]
	output   viperutil.Value[string]
	timeout  viperutil.Value[time.Duration]
}

// AddStatusCommand registers the status subcommand.
func AddStatusCommand(clusterCmd *cobra.Command) {
	reg := viperutil.NewRegistry()
	sc := &statusCmd{
		cell: viperutil.Configure(reg, "cell", viperutil.Options[string]{
			Default: "", FlagName: "cell",
		}),
		database: viperutil.Configure(reg, "database", viperutil.Options[string]{
			Default: "", FlagName: "database",
		}),
		output: viperutil.Configure(reg, "output", viperutil.Options[string]{
			Default: outputText, FlagName: "output",
		}),
		timeout: viperutil.Configure(reg, "timeout", viperutil.Options[time.Duration]{
			Default: 30 * time.Second, FlagName: "timeout",
		}),
	}

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster health",
		Long: `Display the current health and status of the Multigres cluster.

Discovers every cell, gateway, orchestrator and pooler registered in the
topology through the multiadmin server, then asks each pooler for its live
status. Poolers are grouped by database and shard so the current primary,
its replicas, and their replication state are visible at a glance.

Pooler health is derived from what the pooler reports about PostgreSQL
(process running, accepting connections, observed PRIMARY/STANDBY state,
WAL receiver state) combined with the routing role and lifecycle recorded
in the topology. A pooler that cannot be reached is reported as
UNREACHABLE rather than assumed healthy.

Gateway reachability is probed by the multiadmin server. Orchestrators are
listed from the topology only.

Examples:

  # Whole cluster, human-readable
  multigres cluster status

  # One cell, machine-readable
  multigres cluster status --cell zone1 --output json

  # Only poolers serving one database
  multigres cluster status --database postgres`,
		RunE: sc.run,
	}

	cmd.Flags().String("cell", sc.cell.Default(), "Only show components in this cell")
	cmd.Flags().String("database", sc.database.Default(), "Only show poolers serving this database")
	cmd.Flags().String("output", sc.output.Default(), "Output format: text or json")
	cmd.Flags().Duration("timeout", sc.timeout.Default(), "Overall timeout for collecting status")
	cmd.Flags().String("admin-server", "", "host:port of the multiadmin server (overrides config)")

	viperutil.BindFlags(cmd.Flags(), sc.cell, sc.database, sc.output, sc.timeout)

	clusterCmd.AddCommand(cmd)
}

func (sc *statusCmd) run(cmd *cobra.Command, _ []string) error {
	output := strings.ToLower(sc.output.Get())
	if output != outputText && output != outputJSON {
		return fmt.Errorf("invalid --output %q: must be %q or %q", sc.output.Get(), outputText, outputJSON)
	}

	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(cmd.Context(), sc.timeout.Get())
	defer cancel()

	report, err := collectClusterStatus(ctx, client, sc.cell.Get(), sc.database.Get())
	if err != nil {
		return err
	}

	if output == outputJSON {
		return writeJSON(cmd.OutOrStdout(), report)
	}
	return writeText(cmd.OutOrStdout(), report)
}

// statusClient is the subset of the multiadmin client used to collect status.
type statusClient interface {
	GetCellNames(ctx context.Context, in *multiadminpb.GetCellNamesRequest, opts ...grpc.CallOption) (*multiadminpb.GetCellNamesResponse, error)
	GetGateways(ctx context.Context, in *multiadminpb.GetGatewaysRequest, opts ...grpc.CallOption) (*multiadminpb.GetGatewaysResponse, error)
	GetOrchs(ctx context.Context, in *multiadminpb.GetOrchsRequest, opts ...grpc.CallOption) (*multiadminpb.GetOrchsResponse, error)
	GetPoolers(ctx context.Context, in *multiadminpb.GetPoolersRequest, opts ...grpc.CallOption) (*multiadminpb.GetPoolersResponse, error)
	GetPoolerStatus(ctx context.Context, in *multiadminpb.GetPoolerStatusRequest, opts ...grpc.CallOption) (*multiadminpb.GetPoolerStatusResponse, error)
}

// ClusterStatus is the aggregated, serializable view of the cluster.
type ClusterStatus struct {
	Cells     []CellStatus     `json:"cells"`
	Databases []DatabaseStatus `json:"databases"`
	Summary   Summary          `json:"summary"`
	// Warnings are non-fatal collection problems (e.g. a cell whose
	// topology could not be read). Results may be partial when non-empty.
	Warnings []string `json:"warnings,omitempty"`
}

// Summary counts components by health so automation can alert on a
// single field.
type Summary struct {
	Poolers        int `json:"poolers"`
	HealthyPoolers int `json:"healthy_poolers"`
	Gateways       int `json:"gateways"`
	// ReachableGateways is -1 when the admin server did not report
	// reachability.
	ReachableGateways int `json:"reachable_gateways"`
	Orchestrators     int `json:"orchestrators"`
	// ShardsWithoutPrimary lists database/table_group/shard keys that have
	// no pooler currently advertising itself as the writable primary.
	ShardsWithoutPrimary []string `json:"shards_without_primary,omitempty"`
}

// CellStatus lists the infrastructure components registered in one cell.
type CellStatus struct {
	Name          string             `json:"name"`
	Gateways      []GatewayStatus    `json:"gateways"`
	Orchestrators []OrchestratorInfo `json:"orchestrators"`
	PoolerCount   int                `json:"pooler_count"`
}

// GatewayStatus is a multigateway registration plus admin-server probed
// reachability.
type GatewayStatus struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	// PostgresAddress is the client-facing address, when advertised.
	PostgresAddress string `json:"postgres_address,omitempty"`
	Reachable       bool   `json:"reachable"`
}

// OrchestratorInfo is a multiorch registration. Orchestrators are not
// probed: the admin API exposes no liveness signal for them.
type OrchestratorInfo struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

// DatabaseStatus groups shards belonging to one database.
type DatabaseStatus struct {
	Name   string        `json:"name"`
	Shards []ShardStatus `json:"shards"`
}

// ShardStatus groups the poolers of one shard and identifies the primary.
type ShardStatus struct {
	TableGroup string `json:"table_group"`
	Shard      string `json:"shard"`
	// Primary is "cell/name" of the pooler advertising ROUTING_ROLE_PRIMARY,
	// empty if none does.
	Primary string         `json:"primary,omitempty"`
	Poolers []PoolerStatus `json:"poolers"`
}

// PoolerHealth summarizes a pooler for operators.
type PoolerHealth string

const (
	// PoolerHealthy: reachable, postgres accepting connections, serving,
	// observed postgres state matches the advertised routing role, and a
	// standby is streaming from its primary.
	PoolerHealthy PoolerHealth = "HEALTHY"
	// PoolerDegraded: reachable but something operators should look at
	// (postgres not ready, not serving, replication not streaming, role
	// mismatch, lifecycle not ACTIVE).
	PoolerDegraded PoolerHealth = "DEGRADED"
	// PoolerUnreachable: the admin server could not obtain status from the
	// pooler.
	PoolerUnreachable PoolerHealth = "UNREACHABLE"
)

// PoolerStatus combines topology metadata with the pooler's live status.
type PoolerStatus struct {
	Cell    string `json:"cell"`
	Name    string `json:"name"`
	Address string `json:"address"`
	// RoutingRole is the role advertised in topology (PRIMARY, REPLICA,
	// UNKNOWN). This is what routing decisions are based on.
	RoutingRole string `json:"routing_role"`
	// PostgresStatus is what postgres actually is right now (PRIMARY,
	// STANDBY, PROMOTING, STARTING, UNKNOWN). Differs from RoutingRole
	// during transitions.
	PostgresStatus  string       `json:"postgres_status,omitempty"`
	ServingStatus   string       `json:"serving_status"`
	LifecycleStatus string       `json:"lifecycle_status"`
	Health          PoolerHealth `json:"health"`
	// Reasons explains a non-HEALTHY health value.
	Reasons []string `json:"reasons,omitempty"`

	PostgresRunning bool   `json:"postgres_running"`
	PostgresReady   bool   `json:"postgres_ready"`
	WALPosition     string `json:"wal_position,omitempty"`
	PostgresAction  string `json:"postgres_action,omitempty"`

	Primary     *PrimaryDetails     `json:"primary,omitempty"`
	Replication *ReplicationDetails `json:"replication,omitempty"`

	// ConsensusTerm and ConsensusLeader come from the pooler's last decided
	// shard rule, when known.
	ConsensusTerm   int64  `json:"consensus_term,omitempty"`
	ConsensusLeader string `json:"consensus_leader,omitempty"`
	// Error is the collection error when Health is UNREACHABLE.
	Error string `json:"error,omitempty"`
}

// PrimaryDetails is populated for a pooler whose postgres is a primary.
type PrimaryDetails struct {
	LSN                string   `json:"lsn,omitempty"`
	Ready              bool     `json:"ready"`
	ConnectedFollowers []string `json:"connected_followers,omitempty"`
}

// ReplicationDetails is populated for a pooler whose postgres is a standby.
type ReplicationDetails struct {
	UpstreamPrimary   string `json:"upstream_primary,omitempty"`
	WALReceiverStatus string `json:"wal_receiver_status,omitempty"`
	LastReceiveLSN    string `json:"last_receive_lsn,omitempty"`
	LastReplayLSN     string `json:"last_replay_lsn,omitempty"`
	Lag               string `json:"lag,omitempty"`
	ReplayPaused      bool   `json:"replay_paused"`
}

// collectClusterStatus discovers components through the admin server and
// gathers per-pooler live status. Topology read failures for individual
// cells are recorded as warnings so a partially-broken cluster still
// produces a report.
func collectClusterStatus(ctx context.Context, client statusClient, cellFilter, dbFilter string) (*ClusterStatus, error) {
	report := &ClusterStatus{}

	var cells []string
	if cellFilter != "" {
		cells = []string{cellFilter}
	} else {
		resp, err := client.GetCellNames(ctx, &multiadminpb.GetCellNamesRequest{})
		if err != nil {
			return nil, fmt.Errorf("failed to get cell names: %w", err)
		}
		cells = resp.GetNames()
	}
	sort.Strings(cells)

	// Poolers are the point of the report, so failing to list them is fatal.
	// Gateway and orchestrator listings are supporting context and degrade to
	// a warning so a partially-broken topology still yields a report.
	poolerResp, err := client.GetPoolers(ctx, &multiadminpb.GetPoolersRequest{Cells: cells, Database: dbFilter})
	if err != nil {
		return nil, fmt.Errorf("failed to get poolers: %w", err)
	}

	gwResp, err := client.GetGateways(ctx, &multiadminpb.GetGatewaysRequest{Cells: cells})
	if err != nil {
		report.Warnings = append(report.Warnings, fmt.Sprintf("gateways: %v", err))
	}
	// Reachability is probed server-side (the CLI may not be able to reach
	// gateway hosts itself): the set difference between all registered
	// gateways and the only_reachable listing marks the stale ones.
	reachableGateways := map[string]bool{}
	reachabilityKnown := false
	if len(gwResp.GetGateways()) > 0 {
		reachableResp, rerr := client.GetGateways(ctx, &multiadminpb.GetGatewaysRequest{Cells: cells, OnlyReachable: true})
		if rerr != nil {
			report.Warnings = append(report.Warnings, fmt.Sprintf("gateway reachability: %v", rerr))
		} else {
			reachabilityKnown = true
			for _, gw := range reachableResp.GetGateways() {
				reachableGateways[idKey(gw.GetId())] = true
			}
		}
	}

	orchResp, err := client.GetOrchs(ctx, &multiadminpb.GetOrchsRequest{Cells: cells})
	if err != nil {
		report.Warnings = append(report.Warnings, fmt.Sprintf("orchestrators: %v", err))
	}

	cellIndex := map[string]*CellStatus{}
	for _, name := range cells {
		cellIndex[name] = &CellStatus{Name: name}
	}
	cellFor := func(name string) *CellStatus {
		if c, ok := cellIndex[name]; ok {
			return c
		}
		c := &CellStatus{Name: name}
		cellIndex[name] = c
		cells = append(cells, name)
		return c
	}

	for _, gw := range gwResp.GetGateways() {
		gs := GatewayStatus{
			Name:      gw.GetId().GetName(),
			Address:   hostPort(gw.GetHostname(), gw.GetPortMap(), "grpc"),
			Reachable: !reachabilityKnown || reachableGateways[idKey(gw.GetId())],
		}
		if pgAddr := hostPort(gw.GetHostname(), gw.GetPortMap(), "postgres"); pgAddr != "" {
			gs.PostgresAddress = pgAddr
		}
		c := cellFor(gw.GetId().GetCell())
		c.Gateways = append(c.Gateways, gs)
	}
	for _, orch := range orchResp.GetOrchs() {
		c := cellFor(orch.GetId().GetCell())
		c.Orchestrators = append(c.Orchestrators, OrchestratorInfo{
			Name:    orch.GetId().GetName(),
			Address: hostPort(orch.GetHostname(), orch.GetPortMap(), "grpc"),
		})
	}

	poolers := poolerResp.GetPoolers()
	statuses := fetchPoolerStatuses(ctx, client, poolers)

	shards := map[string]*ShardStatus{}
	dbShards := map[string][]*ShardStatus{}
	for i, p := range poolers {
		cellFor(p.GetId().GetCell()).PoolerCount++
		ps := buildPoolerStatus(p, statuses[i].resp, statuses[i].err)

		sk := p.GetShardKey()
		key := shardKeyString(sk)
		shard, ok := shards[key]
		if !ok {
			shard = &ShardStatus{TableGroup: sk.GetTableGroup(), Shard: sk.GetShard()}
			shards[key] = shard
			dbShards[sk.GetDatabase()] = append(dbShards[sk.GetDatabase()], shard)
		}
		if ps.RoutingRole == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY.String() {
			shard.Primary = ps.Cell + "/" + ps.Name
		}
		shard.Poolers = append(shard.Poolers, ps)

		report.Summary.Poolers++
		if ps.Health == PoolerHealthy {
			report.Summary.HealthyPoolers++
		}
	}

	// Assemble sorted output.
	sort.Strings(cells)
	for _, name := range cells {
		c := cellIndex[name]
		sort.Slice(c.Gateways, func(i, j int) bool { return c.Gateways[i].Name < c.Gateways[j].Name })
		sort.Slice(c.Orchestrators, func(i, j int) bool { return c.Orchestrators[i].Name < c.Orchestrators[j].Name })
		report.Summary.Gateways += len(c.Gateways)
		report.Summary.Orchestrators += len(c.Orchestrators)
		for _, gw := range c.Gateways {
			if gw.Reachable {
				report.Summary.ReachableGateways++
			}
		}
		report.Cells = append(report.Cells, *c)
	}
	if !reachabilityKnown {
		report.Summary.ReachableGateways = -1
	}

	dbNames := make([]string, 0, len(dbShards))
	for name := range dbShards {
		dbNames = append(dbNames, name)
	}
	sort.Strings(dbNames)
	for _, name := range dbNames {
		db := DatabaseStatus{Name: name}
		for _, s := range dbShards[name] {
			sort.Slice(s.Poolers, func(a, b int) bool {
				pa, pb := s.Poolers[a], s.Poolers[b]
				// Primary first, then by cell/name for stable output.
				if (pa.RoutingRole == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY.String()) != (pb.RoutingRole == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY.String()) {
					return pa.RoutingRole == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY.String()
				}
				if pa.Cell != pb.Cell {
					return pa.Cell < pb.Cell
				}
				return pa.Name < pb.Name
			})
			if s.Primary == "" {
				report.Summary.ShardsWithoutPrimary = append(report.Summary.ShardsWithoutPrimary,
					name+"/"+s.TableGroup+"/"+s.Shard)
			}
			db.Shards = append(db.Shards, *s)
		}
		sort.Slice(db.Shards, func(i, j int) bool {
			if db.Shards[i].TableGroup != db.Shards[j].TableGroup {
				return db.Shards[i].TableGroup < db.Shards[j].TableGroup
			}
			return db.Shards[i].Shard < db.Shards[j].Shard
		})
		sort.Strings(report.Summary.ShardsWithoutPrimary)
		report.Databases = append(report.Databases, db)
	}

	return report, nil
}

type poolerStatusResult struct {
	resp *multiadminpb.GetPoolerStatusResponse
	err  error
}

// fetchPoolerStatuses queries every pooler concurrently; a slow or dead
// pooler must not serialize the whole report.
func fetchPoolerStatuses(ctx context.Context, client statusClient, poolers []*clustermetadatapb.Multipooler) []poolerStatusResult {
	results := make([]poolerStatusResult, len(poolers))
	var wg sync.WaitGroup
	for i, p := range poolers {
		wg.Add(1)
		go func(i int, p *clustermetadatapb.Multipooler) {
			defer wg.Done()
			resp, err := client.GetPoolerStatus(ctx, &multiadminpb.GetPoolerStatusRequest{
				PoolerId: &clustermetadatapb.ID{Cell: p.GetId().GetCell(), Name: p.GetId().GetName()},
			})
			results[i] = poolerStatusResult{resp: resp, err: err}
		}(i, p)
	}
	wg.Wait()
	return results
}

// buildPoolerStatus merges topology metadata with the live status response
// and derives a health verdict.
func buildPoolerStatus(p *clustermetadatapb.Multipooler, resp *multiadminpb.GetPoolerStatusResponse, err error) PoolerStatus {
	ps := PoolerStatus{
		Cell:            p.GetId().GetCell(),
		Name:            p.GetId().GetName(),
		Address:         hostPort(p.GetHostname(), p.GetPortMap(), "grpc"),
		RoutingRole:     p.GetRoutingState().GetRole().String(),
		ServingStatus:   p.GetServingStatus().String(),
		LifecycleStatus: p.GetLifecycleStatus().GetStatus().String(),
	}

	if err != nil {
		ps.Health = PoolerUnreachable
		ps.Error = err.Error()
		return ps
	}

	st := resp.GetStatus()
	ps.PostgresStatus = st.GetPostgresStatus().String()
	ps.PostgresRunning = st.GetPostgresRunning()
	ps.PostgresReady = st.GetPostgresReady()
	ps.WALPosition = st.GetWalPosition()
	if st.GetPostgresAction() != multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED {
		ps.PostgresAction = st.GetPostgresAction().String()
		if d := st.GetPostgresActionDuration(); d != nil {
			ps.PostgresAction += " (" + d.AsDuration().Truncate(time.Second).String() + ")"
		}
	}
	if decision := resp.GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision(); decision != nil {
		ps.ConsensusTerm = decision.GetRuleNumber().GetCoordinatorTerm()
		if leader := decision.GetLeaderId(); leader != nil {
			ps.ConsensusLeader = idKey(leader)
		}
	}

	if prim := st.GetPrimaryStatus(); prim != nil {
		pd := &PrimaryDetails{LSN: prim.GetLsn(), Ready: prim.GetReady()}
		for _, f := range prim.GetConnectedFollowers() {
			pd.ConnectedFollowers = append(pd.ConnectedFollowers, idKey(f))
		}
		sort.Strings(pd.ConnectedFollowers)
		ps.Primary = pd
	}
	if rep := st.GetReplicationStatus(); rep != nil {
		rd := &ReplicationDetails{
			WALReceiverStatus: rep.GetWalReceiverStatus(),
			LastReceiveLSN:    rep.GetLastReceiveLsn(),
			LastReplayLSN:     rep.GetLastReplayLsn(),
			ReplayPaused:      rep.GetIsWalReplayPaused(),
		}
		if ci := rep.GetPrimaryConnInfo(); ci != nil && ci.GetHost() != "" {
			rd.UpstreamPrimary = net.JoinHostPort(ci.GetHost(), strconv.Itoa(int(ci.GetPort())))
		}
		if lag := rep.GetLag(); lag != nil {
			rd.Lag = lag.AsDuration().Truncate(time.Millisecond).String()
		}
		ps.Replication = rd
	}

	ps.Health, ps.Reasons = derivePoolerHealth(p, st)
	return ps
}

// derivePoolerHealth turns observed postgres state plus topology role into a
// verdict. The advertised routing role alone says nothing about whether
// postgres is up, so every check is against fields the pooler measured.
func derivePoolerHealth(p *clustermetadatapb.Multipooler, st *multipoolermanagerdatapb.Status) (PoolerHealth, []string) {
	var reasons []string

	switch lc := p.GetLifecycleStatus().GetStatus(); lc {
	case clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN:
	default:
		r := "lifecycle " + lc.String()
		if reason := p.GetLifecycleStatus().GetReason(); reason != "" {
			r += ": " + reason
		}
		reasons = append(reasons, r)
	}

	if !st.GetPostgresReady() {
		if st.GetPostgresRunning() {
			reasons = append(reasons, "postgres running but not accepting connections")
		} else {
			reasons = append(reasons, "postgres not running")
		}
	}

	if p.GetServingStatus() != clustermetadatapb.PoolerServingStatus_SERVING {
		reasons = append(reasons, "not serving ("+p.GetServingStatus().String()+")")
	}

	role := p.GetRoutingState().GetRole()
	pg := st.GetPostgresStatus()
	switch role {
	case clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY:
		if pg != multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY {
			reasons = append(reasons, "routed as PRIMARY but postgres is "+pg.String())
		}
		if st.GetPrimaryStatus() != nil && !st.GetPrimaryStatus().GetReady() {
			reasons = append(reasons, "primary not ready")
		}
	case clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA:
		if pg == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY {
			reasons = append(reasons, "routed as REPLICA but postgres is PRIMARY")
		}
		if rep := st.GetReplicationStatus(); rep != nil {
			if rep.GetWalReceiverStatus() != "streaming" {
				recv := rep.GetWalReceiverStatus()
				if recv == "" {
					recv = "not running"
				}
				reasons = append(reasons, "WAL receiver "+recv)
			}
			if rep.GetIsWalReplayPaused() {
				reasons = append(reasons, "WAL replay paused")
			}
		} else if st.GetPostgresReady() {
			reasons = append(reasons, "no replication status")
		}
	default:
		reasons = append(reasons, "routing role unknown")
	}

	if len(reasons) == 0 {
		return PoolerHealthy, nil
	}
	return PoolerDegraded, reasons
}

func writeJSON(w io.Writer, report *ClusterStatus) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func writeText(w io.Writer, report *ClusterStatus) error {
	var b strings.Builder

	fmt.Fprintf(&b, "Cells (%d)\n", len(report.Cells))
	for _, c := range report.Cells {
		fmt.Fprintf(&b, "  %s: %d gateway(s), %d orchestrator(s), %d pooler(s)\n",
			c.Name, len(c.Gateways), len(c.Orchestrators), c.PoolerCount)
		for _, gw := range c.Gateways {
			state := "reachable"
			if !gw.Reachable {
				state = "UNREACHABLE"
			}
			line := fmt.Sprintf("    gateway  %-24s %-24s %s", gw.Name, gw.Address, state)
			if gw.PostgresAddress != "" {
				line += "  pg=" + gw.PostgresAddress
			}
			b.WriteString(line + "\n")
		}
		for _, o := range c.Orchestrators {
			fmt.Fprintf(&b, "    orch     %-24s %s\n", o.Name, o.Address)
		}
	}

	if len(report.Databases) == 0 {
		b.WriteString("\nNo poolers found\n")
	}
	for _, db := range report.Databases {
		fmt.Fprintf(&b, "\nDatabase %s\n", db.Name)
		for _, s := range db.Shards {
			primary := s.Primary
			if primary == "" {
				primary = "NONE"
			}
			fmt.Fprintf(&b, "  Shard %s/%s  primary=%s\n", s.TableGroup, s.Shard, primary)
			for _, p := range s.Poolers {
				writePoolerText(&b, p)
			}
		}
	}

	sm := report.Summary
	b.WriteString("\nSummary\n")
	fmt.Fprintf(&b, "  poolers:       %d/%d healthy\n", sm.HealthyPoolers, sm.Poolers)
	if sm.ReachableGateways >= 0 {
		fmt.Fprintf(&b, "  gateways:      %d/%d reachable\n", sm.ReachableGateways, sm.Gateways)
	} else {
		fmt.Fprintf(&b, "  gateways:      %d\n", sm.Gateways)
	}
	fmt.Fprintf(&b, "  orchestrators: %d\n", sm.Orchestrators)
	for _, s := range sm.ShardsWithoutPrimary {
		fmt.Fprintf(&b, "  WARNING: shard %s has no primary\n", s)
	}
	for _, wmsg := range report.Warnings {
		fmt.Fprintf(&b, "  WARNING: %s\n", wmsg)
	}

	_, err := io.WriteString(w, b.String())
	return err
}

func writePoolerText(b *strings.Builder, p PoolerStatus) {
	role := strings.TrimPrefix(p.RoutingRole, "ROUTING_ROLE_")
	pg := strings.TrimPrefix(p.PostgresStatus, "POSTGRES_STATUS_")
	if pg == "" {
		pg = "-"
	}
	line := fmt.Sprintf("    %-8s %-24s %-24s %-12s pg=%-10s %s",
		role, p.Cell+"/"+p.Name, p.Address, string(p.Health), pg, p.WALPosition)
	b.WriteString(strings.TrimRight(line, " ") + "\n")

	details := make([]string, 0, 4)
	if p.Primary != nil {
		if len(p.Primary.ConnectedFollowers) > 0 {
			details = append(details, "followers="+strings.Join(p.Primary.ConnectedFollowers, ","))
		} else {
			details = append(details, "followers=none")
		}
	}
	if p.Replication != nil {
		recv := p.Replication.WALReceiverStatus
		if recv == "" {
			recv = "none"
		}
		d := "wal_receiver=" + recv
		if p.Replication.UpstreamPrimary != "" {
			d += " upstream=" + p.Replication.UpstreamPrimary
		}
		if p.Replication.Lag != "" {
			d += " lag=" + p.Replication.Lag
		}
		details = append(details, d)
	}
	if p.PostgresAction != "" {
		details = append(details, "action="+p.PostgresAction)
	}
	if p.ConsensusTerm != 0 {
		d := "term=" + strconv.FormatInt(p.ConsensusTerm, 10)
		if p.ConsensusLeader != "" {
			d += " leader=" + p.ConsensusLeader
		}
		details = append(details, d)
	}
	for _, d := range details {
		fmt.Fprintf(b, "%s%s\n", strings.Repeat(" ", 13), d)
	}
	for _, r := range p.Reasons {
		fmt.Fprintf(b, "%s! %s\n", strings.Repeat(" ", 13), r)
	}
	if p.Error != "" {
		fmt.Fprintf(b, "%s! %s\n", strings.Repeat(" ", 13), p.Error)
	}
}

func hostPort(host string, ports map[string]int32, name string) string {
	port, ok := ports[name]
	if !ok {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}

func idKey(id *clustermetadatapb.ID) string {
	return id.GetCell() + "/" + id.GetName()
}

func shardKeyString(sk *clustermetadatapb.ShardKey) string {
	return sk.GetDatabase() + "/" + sk.GetTableGroup() + "/" + sk.GetShard()
}
