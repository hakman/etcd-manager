# Proposal: local backups without a shared backup store

Status: analysis / design proposal (no code changes yet)

This document analyses etcd-manager's current backup and recovery
mechanisms, its peer protocol, and its use of the etcd API, and then
evaluates a mode where backups are stored **locally on each node** with
**no shared object store**. When a restore is needed, nodes exchange
information about their latest healthy backup over the existing
etcd-manager gRPC protocol, and the leader uses that information to
start a new cluster from the best available backup.

- [1. Summary and motivation](#1-summary-and-motivation)
- [2. Current state](#2-current-state)
- [3. Design](#3-design)
- [4. Failure-mode matrix](#4-failure-mode-matrix)
- [5. Safety and security considerations](#5-safety-and-security-considerations)
- [6. Compatibility and migration](#6-compatibility-and-migration)
- [7. Alternatives considered](#7-alternatives-considered)
- [8. Implementation sketch and phasing](#8-implementation-sketch-and-phasing)
- [9. Open questions](#9-open-questions)

## 1. Summary and motivation

etcd-manager today requires a shared, durable backup store (S3, GCS,
Azure Blob, Swift, or a shared filesystem path) that **every** node can
read and write. The store carries more than backups: the authoritative
cluster spec, the "cluster created" safety marker, and the
`restore-backup` command queue all live under `<backup-store>/control/`.

Removing that dependency is attractive for several reasons:

- **Credentials**: every control-plane node needs write access to the
  object store; that is a broad standing permission for what is
  conceptually node-local state.
- **Air-gapped / bare-metal clusters**: an object store is not always
  available. Static mode exists for such environments but cannot do
  restores at all today.
- **Cost and latency**: snapshot uploads incur egress; local snapshots
  are essentially free, so they can be taken more frequently, reducing
  the data-loss window of a restore.
- **Consistency workarounds**: the code carries several workarounds for
  object-store eventual consistency (a GET-not-LIST trick, a busy-wait
  for command visibility) that a gossip-based control plane does not
  need.

**Goals**

- Periodic backups stored on each node's local (persistent) volume.
- Nodes advertise their latest healthy backups to peers.
- Disaster recovery — starting a new cluster from the best available
  backup — driven purely over the existing peer protocol.
- A control plane (spec, created-marker, commands) that does not require
  shared storage.

**Non-goals**

- Surviving the *simultaneous* destruction of all nodes **and** all
  volumes. Local-only backups share fate with the fleet; a shared store
  remains the answer for that scenario (see hybrid mode, §3.9).
- Changing the restore data path (snapshot restore + key copy + raft
  replication) — it already works with only one node touching the
  backup bytes.

## 2. Current state

### 2.1 Architecture and peer protocol

Every node runs the same `etcd-manager` binary containing three
components (`cmd/etcd-manager/main.go`):

1. **privateapi gossip server** (`pkg/privateapi`): `ClusterService`
   with `Ping` and `LeaderNotification`. Peers are seeded from a
   discovery mechanism (cloud volume tags, static config, or a seed
   directory) and also learned from inbound pings. Leadership is a weak
   leader election: the **lowest peer ID** among healthy peers bids for
   leadership and must be **acked by every healthy peer**
   (`pkg/privateapi/leadership.go`); the leader resigns whenever a new
   peer appears (`pkg/controller/controller.go:281-293`). Every command
   RPC carries the leadership token and is validated against it
   (`pkg/etcd/etcdserver.go:719-732`).

2. **EtcdServer node agent** (`pkg/etcd`): implements
   `EtcdManagerService` (`pkg/apis/etcd/etcdapi.proto`):
   `GetInfo`, `UpdateEndpoints`, `JoinCluster` (phases
   PREPARE / INITIAL_CLUSTER / JOIN_EXISTING / CANCEL_PREPARE),
   `Reconfigure` (quarantine, version, TLS), `DoBackup`, `DoRestore`,
   `StopEtcd`. Node state is persisted on the data volume: a `state`
   file (protobuf `EtcdState`), `data/<clusterToken>/`,
   `pki/<clusterToken>/`, and `data-trashcan/` (where `StopEtcd`
   archives old data dirs instead of deleting them).

3. **Controller** (`pkg/controller`): a 10-second reconcile loop that
   only acts while leader. Each cycle it calls `GetInfo` on every peer,
   lists and health-checks etcd members, and then walks a decision
   cascade: create new cluster → process restore command → periodic
   backup → quarantine reconciliation → expand/shrink → replace
   empty-disk members → TLS/version reconciliation.

`GetInfoResponse` today reports `cluster_name`, `node_configuration`,
`etcd_state` (cluster token, member list, version, quarantined) and
`disk_empty`. This is the natural extension point for advertising local
backups — the leader already collects it from every peer every cycle.

### 2.2 Backups

- The backup store abstraction (`pkg/backup/store.go`) has a single VFS
  implementation (`pkg/backup/vfs.go`); the storage backend is chosen
  purely by URL scheme through the vendored kops VFS layer. Notably,
  **`file://` URLs and bare filesystem paths already work** — the
  integration tests and the walkthrough use them.
- A backup is a directory `<RFC3339-timestamp>-<sequence>/` containing
  `etcd.backup.gz` (the etcd v3 snapshot, taken via the etcd
  Maintenance `Snapshot` API and streamed through gzip —
  `pkg/etcdclient/client.go:278`) and `_etcd_backup.meta` (JSON
  `BackupInfo`: `etcd_version`, `timestamp`,
  `cluster_spec{member_count, etcd_version}`). The meta file is written
  after the data file and acts as the commit marker; there are **no
  checksums** and **no etcd revision/term** recorded.
- The leader triggers a backup every `--backup-interval` (default 15m)
  by sending `DoBackup{storage: <store URL>}` to the **first healthy
  member** (`pkg/controller/controller.go:569-591,918-946`). The
  receiving node rebuilds the store from the URL in the request and
  uploads using its own ambient credentials — the leader never touches
  the backup bytes. Forced backups are taken before member add/remove
  and around version upgrades.
- Retention (`pkg/backupcontroller/cleanup.go`): keep everything
  newer than 1h, one per hour for 7 days, one per day for ~2 years;
  unparseable names are never deleted.

### 2.3 The control store

`commands.NewStore` (`pkg/commands/vfs.go`) silently namespaces under
`<backup-store>/control/`:

- `etcd-cluster-spec` — the authoritative
  `ClusterSpec{member_count, etcd_version}`. The controller refuses to
  act without it.
- `etcd-cluster-created` — a marker whose **absence** means "new
  cluster". This is the interlock that prevents etcd-manager from
  bootstrapping a fresh empty cluster on top of a total data loss: with
  the marker present and 0 members, the leader loops with
  `etcd has 0 members registered; must issue restore-backup command`
  (`pkg/controller/controller.go:477-483`).
- `<timestamp>-000000/_command.json` — the command queue; the only
  command type consumed is `RestoreBackupCommand{cluster_spec, backup}`,
  written by `etcd-manager-ctl restore-backup` or self-issued by the
  upgrade path.

**Static mode** (`pkg/static/commandstore.go`, `--static-config`) is an
existing precedent for a no-shared-store control plane: the spec is
derived from local config, `IsNewCluster` is a local
`please-create-new-cluster` marker file in the data dir, and commands
are unsupported — which also means **restore is impossible in static
mode today**.

### 2.4 Restore

Restore is **never automatic**. The flow, once a `restore-backup`
command is visible (`pkg/controller/controller.go:399-425`):

1. Preconditions: the etcd version must have a local binary, and
   `ackedPeerCount >= quorumSize(spec.member_count)`.
2. `createNewCluster` (`pkg/controller/newcluster.go`): generate a new
   random cluster token; `StopEtcd` every peer that has state (data is
   archived to `data-trashcan/`, not deleted); two-phase `JoinCluster`
   (PREPARE, then INITIAL_CLUSTER). Every node starts a **fresh, empty**
   etcd, **quarantined** — listening on separate quarantine client URLs
   invisible to normal clients.
3. `restoreBackupAndLiftQuarantine` (`pkg/controller/restore.go`):
   send `DoRestore{storage, backup_name}` to **one** healthy peer.
4. That peer (`pkg/etcd/restore.go`) downloads the snapshot, runs
   `etcdctl snapshot restore` (`etcdutl` for etcd >= 3.6) into a
   throwaway single-node etcd on unix sockets with
   `--force-new-cluster`, then copies keys page-by-page into its live
   quarantined etcd. **Raft replicates the data to the other members**
   — only the restoring peer ever needs access to the backup.
5. The leader rewrites the cluster spec, removes the command, and lifts
   the quarantine on a later cycle once a healthy quorum exists.

The non-in-place upgrade path (`pkg/controller/upgrade.go:60-133`)
reuses this machinery: backup → quarantine → backup again → self-issue
a `RestoreBackupCommand` → busy-wait until the command is visible in
the (eventually-consistent) store → `StopEtcd` everywhere.

### 2.5 What already enables local backups

Three properties make this proposal mostly a control-plane change, not
a storage change:

1. The VFS store already supports plain local paths — a "local backup
   store" is just `backup.NewStore("<localDir>")`.
2. `DoBackupRequest.storage` is per-request — commanding each member to
   back up to its own path is already expressible in the protocol.
3. The restore executor only needs the **restoring peer** to reach the
   store; the rest of the cluster gets the data via raft.

The hard problems are: where the control data (spec, created-marker,
commands) lives, how the leader learns which node has the best backup,
and the durability model.

## 3. Design

### 3.1 Local backup store and retention

- New node-side flag `--local-backup-dir`, default
  `<baseDir>/local-backups` — i.e. **on the same persistent volume as
  the etcd data dir**. In the kops model these volumes are remounted by
  replacement instances, so backups survive instance replacement
  exactly as long as etcd state itself does.
- The leader sends `DoBackup{use_local_store: true}` to **all healthy
  members** each interval (today: one member). With no shared store,
  N independent copies *are* the durability story. Each member
  snapshots its own local etcd (the Maintenance Snapshot API serves
  from the local member), so this does not concentrate load.
- The receiving node resolves its own configured directory; the request
  no longer carries a filesystem path. (Today the node blindly opens
  whatever URL the leader sends — removing RPC-supplied storage paths
  is also a small security improvement.)
- Retention runs **per node** on a timer inside `EtcdServer`, reusing
  the tiering logic of `pkg/backupcontroller/cleanup.go` with tighter
  defaults suitable for finite disks (e.g. all < 1h, hourly for 24h,
  daily for 7d) plus a hard cap (`--local-backup-max-count`). The
  newest backup is always retained regardless of age.
- The backup interval default stays 15m, but local snapshots have no
  egress cost, so operators can lower it to shrink the restore
  data-loss window.

### 3.2 Protocol extensions

All changes are proto3-additive to `pkg/apis/etcd/etcdapi.proto`
(unknown fields are ignored by old nodes; absent fields are tolerated
by a new leader), so mixed-version fleets are safe:

```proto
message BackupInfo {
    string etcd_version = 1;
    int64 timestamp = 2;
    ClusterSpec cluster_spec = 3;
    // NEW: captured via etcd Maintenance.Status immediately before the
    // snapshot; primary ranking key for data recency.
    int64 etcd_revision = 4;
    uint64 raft_term = 5;      // informational only — see §5
    uint64 raft_index = 6;
    string member_name = 7;    // which member took the backup
    string sha256 = 8;         // integrity hash of etcd.backup.gz
}

message LocalBackupSummary {
    string name = 1;           // name within the owning node's local store
    BackupInfo info = 2;
}

// Leader-written, per-node-persisted control state; replaces
// <backup-store>/control/.
message ControlRecord {
    bool cluster_created = 1;
    ClusterSpec cluster_spec = 2;
    int64 cluster_spec_timestamp = 3;      // last-writer-wins merge key
    repeated Command pending_commands = 4;
    // Completed Command.timestamp values; GC'd after 7 days.
    repeated int64 command_tombstones = 5;
}

message GetInfoResponse {
    string cluster_name = 2;
    EtcdNode node_configuration = 5;
    EtcdState etcd_state = 6;
    bool disk_empty = 7;
    // NEW:
    repeated LocalBackupSummary local_backups = 8;  // top 3 by revision
    ControlRecord control = 9;                      // this node's copy
}

message DoBackupRequest  { /* existing fields */ bool use_local_store = 5; }
message DoRestoreRequest { /* existing fields */ bool use_local_store = 5; }

message RestoreBackupCommand {
    ClusterSpec cluster_spec = 1;
    string backup = 3;
    string backup_peer = 4;    // NEW: peer id owning the backup ("" = shared store)
}

service EtcdManagerService {
    // ... existing RPCs ...

    // NEW: leader pushes control records to all peers
    // (leadership-token authorized, like every other command RPC).
    rpc UpdateControl (UpdateControlRequest) returns (UpdateControlResponse);

    // Phase 2 (optional): stream a backup between peers.
    rpc FetchBackup (FetchBackupRequest) returns (stream FetchBackupChunk);
}
```

### 3.3 Backup metadata: revision and checksum

Capturing `etcd_revision` requires exposing the etcd Maintenance
`Status` call in `pkg/etcdclient/client.go` (the maintenance client is
already constructed there; only `Snapshot` and `Status`-for-`LeaderID`
are used today) and calling it in `DoBackupV3`
(`pkg/etcd/backup.go`) right before `SnapshotSave`. The revision is
written into `_etcd_backup.meta`, so ranking works even for backups
taken by a process that later crashed.

`sha256` of `etcd.backup.gz` is computed while streaming the snapshot
and verified before a restore; on mismatch, the leader falls back to
the next-ranked candidate. (The shared store has no integrity check
today — this is a new capability, not just parity.)

### 3.4 Control plane without a shared store

Replace the VFS control store with a **leader-written,
gossip-aggregated control record**:

- Each node persists a `ControlRecord` at
  `<baseDir>/local-control/control.json` and reports it in `GetInfo`.
- The leader pushes updates to all acked peers via the new
  `UpdateControl` RPC (analogous to how `UpdateEndpoints` already
  broadcasts the member map every cycle).
- The leader merges the records it collects each cycle:
  - **cluster_created**: OR across all peers — a peer reporting the
    marker, **or advertising any local backup, or having a non-empty
    data dir**, means the cluster existed. "Any", not quorum,
    deliberately: a false "created" merely requires an operator
    command (safe); a false "new" bootstraps an empty cluster over
    lost data (catastrophic). This is strictly safer than today's
    single marker object.
  - **cluster_spec**: last-writer-wins on `cluster_spec_timestamp`.
    The bootstrap source of truth is configuration (as in static mode
    — kops already renders the desired spec); the leader persists it
    at cluster creation and rewrites it after a restore, replacing
    `SetExpectedClusterSpec`.
  - **pending_commands**: union across peers minus tombstones, keyed
    by `Command.timestamp` (already the de-facto identity in the VFS
    queue's directory naming). On completion the leader broadcasts a
    tombstone. Union semantics are safe because the only command type
    is `RestoreBackup`, which is gated on a 0-member cluster and
    removed on success.
- Implementation shape: a new `commands.Store` implementation
  (`pkg/commands/gossipstore.go`) fed by the controller with the
  freshly gathered cluster state each cycle. The existing
  refresh/invalidate hooks (`InvalidateControlStore`,
  `refreshControlStore`) map naturally onto "re-aggregate from peers".

### 3.5 Issuing commands without a shared store

`etcd-manager-ctl` gains a `--local` mode that writes the command JSON
to a drop directory on **any one node's** volume
(`<baseDir>/local-control/commands-inbox/`). The node agent picks it
up, persists it into its `ControlRecord.pending_commands`, and
advertises it via `GetInfo`; the leader sees it within one gossip
cycle. This follows the static-mode precedent (operator has host
access anyway) and avoids inventing a new client-to-leader credential
story for phase 1. A convenience form `restore-backup best` lets the
leader substitute the top-ranked candidate itself.

### 3.6 Restore decision

Restore **stays operator-triggered**. This matches the project's
existing philosophy — the controller deliberately refuses to recover a
0-member cluster without a command — and it is the right default: no
algorithm can distinguish "the data is gone, restore the backup" from
"temporary total outage, wait for the nodes to come back". An
automatic restore risks resurrecting a stale snapshot over data that
would have survived a reboot.

Leader-side selection, once a restore command is pending:

```
candidates = []
for peer in clusterState.peers where peer.info != nil:
    for b in peer.info.local_backups:
        candidates += {owner: peer, name: b.name, info: b.info}
if sharedStoreConfigured:                 # hybrid mode
    candidates += sharedStore.ListBackups()   # owner = ""

rank by: etcd_revision DESC, timestamp DESC, name DESC

guards:
  - ackedPeerCount >= quorumSize(spec.member_count)   # existing check
  - peer set stable for >= 2 consecutive cycles       # let survivors gossip in
  - if any expected peer is absent: warn, and require an explicit
    force flag on the command (an absent node might hold a newer backup)
  - refuse the command outright if healthy members >= quorum
    (guards against duplicate/stale restore commands)

best = candidates[0]
require: best.owner is an acked, healthy peer (or "" in hybrid mode)
```

**Why revision, not raft term or wall clock.** A node partitioned into
a minority keeps incrementing its raft term through failed elections
while committing nothing — term can be high while data is stale.
Wall-clock timestamps depend on node clocks. `etcd_revision` is the
monotonic count of committed writes: the backup with the highest
revision provably contains the most committed data.

**The dead-node case.** If the node holding the newest backup is down
at restore time, it cannot advertise and is simply not a candidate.
The leader logs which peers were consulted; the operator decides
whether to wait for it, recover its volume, or accept the next-best
backup (bounded staleness = one backup interval). This is inherent to
local storage and is listed as an accepted limitation.

An opt-in `--auto-restore` flag can be offered for unattended
environments, documented as dangerous, gated on: cluster known
created, 0 members, **all** peers reporting empty/trashcanned disks, a
quorum of expected peers present, and a dwell time (e.g. 15 minutes of
continuous 0-member state). Default off.

### 3.7 Restore execution

Send `DoRestore{use_local_store: true, backup_name}` to the **peer
that owns the chosen backup**. The existing executor already does
everything on the restoring peer; only the store construction changes
(own local dir instead of `backup.NewStore(request.Storage)`), and
raft distributes the restored data as today.

Two small controller changes make this reliable:

- `createNewCluster` currently fills the membership proposal from Go
  map iteration order — nondeterministic. When a restore command with
  `backup_peer` is pending, that peer is placed first in the proposal
  so it is guaranteed to be a member (and thus healthy and reachable)
  in the new cluster.
- `restoreBackupAndLiftQuarantine` currently picks an arbitrary
  healthy member. It must instead select the peer matching
  `backup_peer` and error if that peer is not a healthy member.

If the owner dies between command issuance and completion: the command
is only removed on success, so the next cycle re-runs selection;
`createNewCluster` is idempotent (fresh token; old data archived to
the trashcan). With `restore-backup best` the leader falls back to the
next-ranked candidate; with a named backup it errors awaiting the
owner.

A `FetchBackup` streaming RPC (peer-to-peer backup transfer) is
**deferred to phase 2**: it is only needed when the owner cannot be a
member of the new cluster (rare; workaround: restore first, resize
after) and for optional cross-node backup mirroring.

### 3.8 Upgrade path

`stopForUpgrade` records `backup_peer` (the member that took the
quarantined backup) in its self-issued command, which now travels via
`UpdateControl` instead of the VFS queue. The busy-wait for
object-store visibility becomes unnecessary in local mode — one gossip
cycle suffices.

### 3.9 Hybrid mode (recommended default posture)

Local-only backups share fate with the fleet: if every volume is
destroyed simultaneously, both the backups and the created-markers are
gone and the cluster is unrecoverable. Therefore:

- If both `--backup-store` and `--local-backup-dir` are configured,
  run **hybrid mode**: local backups every interval (fast restore
  path, no egress), shared-store backups on a slower cadence
  (durability floor). Restore candidates merge both sources.
- Local-only mode is appropriate when volumes are independently
  durable (EBS-style volumes that outlive instances, or bare-metal
  with RAID/replicated storage) and the operator accepts the
  all-volumes-lost risk.

## 4. Failure-mode matrix

| # | Scenario | Behavior under this design | Verdict |
|---|----------|----------------------------|---------|
| 1 | Single node/volume lost | Its backups are lost too; N-1 copies remain; the normal empty-disk member-replacement path resyncs the node from the leader via raft — no restore involved | OK |
| 2 | All etcd data lost, volumes survive (process wipe, or immutable-infra instance replacement with volume remount) | New instances remount volumes carrying `local-backups/` and `local-control/`; created-marker aggregation blocks auto-bootstrap; operator issues restore; revision ranking picks the best copy | OK — the designed-for case |
| 3 | All nodes **and** all volumes lost simultaneously | Unrecoverable in local-only mode: no backups, no markers | Fundamental limit — use hybrid mode (§3.9) |
| 4 | Fresh fleet attached to old volumes with stale backups | Any advertised backup ⇒ treated as "created" ⇒ no auto-bootstrap; operator decides | OK — safer than today (losing the shared store's marker permits re-bootstrap) |
| 5 | Partition: minority node has stale data but inflated raft term | Ranking uses revision, not term; the minority's revision froze at partition time and ranks lower | OK |
| 6 | Backup taken during quarantine/restore window | Quarantine blocks client writes; backups are leader-commanded, and the leader does not run periodic backups while a restore command is pending (branch ordering) | OK — unchanged |
| 7 | Newest backup is on a node that is down at restore time | Not a candidate; leader logs consulted peers; operator waits or accepts next-best (staleness bounded by the backup interval) | Accepted limitation; dwell + warning + force-flag reduce accidents |
| 8 | Restore command dropped on two nodes / duplicated | Commands merge by timestamp identity; leader refuses restore when healthy members ≥ quorum unless forced; commands execute serially and are tombstoned on success | Mitigated (today's shared queue has the same exposure, without the quorum refusal) |
| 9 | Split-brain leaders during a gossip partition | Unchanged protections: leadership requires acks from all healthy peers, lowest ID wins, and the restore path re-checks `ackedPeerCount >= quorumSize` | OK — inherited |
| 10 | Silent corruption of a local backup | `sha256` verified before restore; on mismatch, fall back to next-ranked candidate | New capability |
| 11 | Disk pressure: backups compete with etcd data for volume space | Per-node retention with hard count cap; `--local-backup-dir` may point at a second volume; document sizing guidance | Mitigated |
| 12 | Mixed-version rolling upgrade of etcd-manager | Old nodes ignore new fields; a new leader sees peers without `local_backups`/`control` and keeps shared-store behavior; local mode is capability-gated on **all** peers advertising support | OK with the gate |
| 13 | Restore resurrects a stale snapshot over newer (lost) data | Unchanged philosophy: restore is explicit operator-acknowledged data loss (see the comment on `RestoreBackupCommand` in the proto); revision ranking and the candidate report minimize the delta | Documented |

## 5. Safety and security considerations

- **Durability model**: backups are exactly as durable as the volumes
  they live on. This must be stated prominently in operator docs; the
  default location on the etcd data volume ties backup survival to
  etcd-state survival, which is the intuitively correct coupling.
- **No RPC-supplied storage paths**: today `DoBackup`/`DoRestore`
  carry a store URL that the node opens blindly (guarded only by the
  leadership token). With `use_local_store`, nodes only ever touch
  their own configured directory.
- **Why restore stays manual**: see §3.6. Automatic restore converts a
  transient outage into permanent data loss in the worst case.
- **Why `created` is OR-of-peers, not quorum**: the marker's only job
  is to prevent bootstrap-over-data-loss; the failure asymmetry
  (annoying vs catastrophic) dictates the most conservative merge.
- **Why revision, not term/wall-clock, ranks backups**: see §3.6.
- **Leadership**: all new RPCs (`UpdateControl`, targeted `DoRestore`)
  reuse the existing leadership-token authorization. Note the optional
  file-lock leader lock is plumbed but currently nil in `main.go`;
  split-brain protection remains the ack-all-peers rule, unchanged by
  this proposal.

## 6. Compatibility and migration

- **Wire compatibility**: all proto changes are additive (proto3);
  mixed fleets are safe.
- **Capability gating**: the leader only operates in local mode when
  every acked peer advertises local-backup support (non-empty
  `control`/`local_backups` handshake); otherwise it falls back to
  shared-store behavior.
- **Migration of an existing cluster**: enable `--local-backup-dir`
  fleet-wide (hybrid mode) → nodes begin taking local backups and the
  leader seeds each node's `ControlRecord` from the shared control
  store (spec, created-marker) → once all peers advertise records,
  `--backup-store` may be dropped (local-only) or kept (hybrid).
  Rollback is the reverse; the shared store is never deleted by
  etcd-manager.
- **`etcd-manager-ctl`**: existing store-based subcommands keep
  working in hybrid mode; `--local` covers local-only clusters.

## 7. Alternatives considered

- **Automatic restore by default** — rejected: cannot distinguish data
  loss from transient outage; kept as a heavily-gated opt-in.
- **Quorum-of-peers for the created-marker** — rejected: OR is
  strictly safer given the failure asymmetry (§5).
- **Storing spec/marker/commands inside etcd itself** — rejected as
  primary: the control data is needed exactly when etcd is down or
  empty; circular. A mirrored copy inside etcd is fine as
  belt-and-braces.
- **Restore via a `FetchBackup` transfer to a leader-chosen node** —
  unnecessary for the core mechanism: raft already replicates the
  restored data from whichever member performs the restore. Kept as a
  phase-2 option for mirroring and edge cases.
- **Per-node backups into the shared store, no control changes** — a
  half-measure: keeps the credential/egress/consistency problems and
  doesn't help air-gapped clusters.

## 8. Implementation sketch and phasing

**Phase 1** — local store, protocol, manual restore:

- `pkg/apis/etcd/etcdapi.proto` (+ regenerated `etcdapi.pb.go`):
  `BackupInfo` fields, `LocalBackupSummary`, `ControlRecord`,
  `GetInfoResponse` extensions, `use_local_store` flags,
  `backup_peer`, `UpdateControl`.
- `pkg/etcdclient/client.go`: expose Maintenance `Status()` (revision,
  term, index).
- `pkg/etcd/backup.go`: capture revision + sha256 into `BackupInfo`.
- `pkg/etcd/etcdserver.go`: advertise local backups and the control
  record in `GetInfo`; resolve the local store in
  `DoBackup`/`DoRestore`; `UpdateControl` handler; command inbox
  watcher; local retention timer.
- `pkg/commands/gossipstore.go` (new): `commands.Store` implementation
  over aggregated `ControlRecord`s.
- `pkg/controller/controller.go`: back up all healthy members;
  aggregate created/spec/commands from peers; candidate ranking;
  capability gate.
- `pkg/controller/restore.go`: target `backup_peer`; checksum
  verification and fallback.
- `pkg/controller/newcluster.go`: deterministic proposal including the
  backup owner.
- `pkg/controller/upgrade.go`: `backup_peer` in the self-issued
  command; drop the store busy-wait in local mode.
- `pkg/backupcontroller/cleanup.go`: node-local retention profile.
- `cmd/etcd-manager/main.go`: `--local-backup-dir`, retention flags,
  mode wiring (`--backup-store` becomes optional).
- `cmd/etcd-manager-ctl/main.go`: `--local` drop mode,
  `restore-backup best`.
- Tests: extend `test/integration/backuprestore` with a local-mode
  variant (wipe all node processes, keep volumes, drop a restore
  command in one inbox, assert recovery from the highest-revision
  backup); unit tests for ranking and record merging.

**Phase 2** — `FetchBackup` streaming RPC (cross-node mirroring,
restore when the owner can't be a member), opt-in `--auto-restore`,
`etcd-manager-ctl` over gRPC with a proper credential story.

## 9. Open questions

- Should local backups be mirrored to K peers (via `FetchBackup`) so
  the loss of one volume can't lose the newest backup? Trades disk and
  network for a smaller staleness window in failure mode 7.
- Exact retention defaults and disk-usage guardrails (pause backups
  below a free-space threshold?).
- Should the leader periodically verify local backup checksums
  (scrubbing), or only at restore time?
- Whether `restore-backup best` should require an explicit
  `--max-staleness` bound before accepting a candidate older than the
  last known healthy write.
