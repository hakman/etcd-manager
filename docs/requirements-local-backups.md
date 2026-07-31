# Requirements: local backups without a shared backup store

Status: draft, extracted from [proposal-local-backups.md](proposal-local-backups.md)

Each requirement has an ID, a priority (MoSCoW: MUST / SHOULD / MAY),
and a trace to the proposal section it derives from. "Local mode" means
operation without a configured shared backup store; "hybrid mode" means
both local and shared stores are configured.

## 1. Scope

Enable etcd-manager to take and retain etcd backups on each node's
local persistent volume, exchange backup availability information over
the existing peer protocol, and perform disaster recovery (starting a
new cluster from the best available backup) without any shared object
store.

Out of scope (explicit non-goals):

- N1. Surviving simultaneous destruction of all nodes and all volumes
  in local-only mode (proposal §1, §3.9).
- N2. Changing the restore data path (snapshot restore + key copy +
  raft replication) (proposal §1).
- N3. Peer-to-peer backup streaming (`FetchBackup`), automatic restore,
  and a gRPC credential story for `etcd-manager-ctl` — deferred to
  phase 2 (proposal §3.7, §3.6, §8).

## 2. Functional requirements

### 2.1 Backup creation (FR-B)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-B1 | Each node SHALL support a local backup store at a configurable directory (`--local-backup-dir`), defaulting to `<baseDir>/local-backups` on the etcd data volume. | MUST | §3.1 |
| FR-B2 | The local store SHALL reuse the existing `backup.Store` VFS implementation and on-disk backup format (`etcd.backup.gz` + `_etcd_backup.meta`). | MUST | §2.5, §3.1 |
| FR-B3 | In local mode, the leader SHALL command **all healthy members** (not one) to take a local backup each backup interval, via `DoBackup{use_local_store: true}`. | MUST | §3.1 |
| FR-B4 | A node receiving `DoBackup{use_local_store: true}` SHALL resolve its own configured backup directory and SHALL ignore/reject any RPC-supplied storage path. | MUST | §3.1, §5 |
| FR-B5 | Each node SHALL run backup retention locally on a timer (reusing the tiered cleanup logic) with defaults suitable for finite disks and a hard count cap (`--local-backup-max-count`). | MUST | §3.1 |
| FR-B6 | Retention SHALL always preserve the newest local backup regardless of age. | MUST | §3.1 |
| FR-B7 | The backup interval SHALL remain configurable (`--backup-interval`, default 15m); documentation SHOULD note that local snapshots permit shorter intervals. | SHOULD | §3.1 |

### 2.2 Backup metadata (FR-M)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-M1 | `BackupInfo` SHALL record the etcd **revision** captured via the etcd Maintenance Status API immediately before the snapshot. | MUST | §3.2, §3.3 |
| FR-M2 | `BackupInfo` SHALL record a `sha256` integrity hash of `etcd.backup.gz`, computed while streaming the snapshot. | MUST | §3.2, §3.3 |
| FR-M3 | `BackupInfo` SHOULD additionally record `raft_term`, `raft_index`, and the taking member's name, for diagnostics only (never for ranking). | SHOULD | §3.2, §5 |
| FR-M4 | The etcd client wrapper SHALL expose the Maintenance `Status()` call (revision, term, index). | MUST | §3.3 |

### 2.3 Backup advertisement / protocol (FR-P)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-P1 | `GetInfoResponse` SHALL be extended with `repeated LocalBackupSummary local_backups` listing the node's top backups (bounded, e.g. 3), ordered newest-revision first. | MUST | §3.2 |
| FR-P2 | `GetInfoResponse` SHALL be extended with a `ControlRecord control` field carrying the node's persisted control state. | MUST | §3.2, §3.4 |
| FR-P3 | All protobuf changes SHALL be proto3-additive (no field renumbering/removal) so that mixed-version fleets interoperate. | MUST | §3.2, §6 |
| FR-P4 | A new leader-authorized `UpdateControl` RPC SHALL allow the leader to push control records to all peers; it SHALL use the existing leadership-token authorization (`validateHeader`). | MUST | §3.2, §3.4, §5 |
| FR-P5 | `RestoreBackupCommand` SHALL gain a `backup_peer` field identifying the peer owning the backup (empty = shared store). | MUST | §3.2 |

### 2.4 Control plane without a shared store (FR-C)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-C1 | Each node SHALL persist a `ControlRecord` (cluster-created flag, cluster spec + timestamp, pending commands, command tombstones) at `<baseDir>/local-control/control.json`. | MUST | §3.4 |
| FR-C2 | A new `commands.Store` implementation SHALL aggregate `ControlRecord`s gathered from peers each controller cycle, behind the existing store interface. | MUST | §3.4 |
| FR-C3 | The merged **cluster-created** state SHALL be the OR across all peers; a peer advertising any local backup or a non-empty data dir SHALL also imply "created". | MUST | §3.4, §5 |
| FR-C4 | The merged **cluster spec** SHALL be last-writer-wins on `cluster_spec_timestamp`; the bootstrap source of truth is configuration (static-mode style). | MUST | §3.4 |
| FR-C5 | The merged **command queue** SHALL be the union of pending commands across peers minus tombstones, keyed by `Command.timestamp`; the leader SHALL broadcast a tombstone on completion; tombstones SHALL be garbage-collected after a bounded period (e.g. 7 days). | MUST | §3.4 |
| FR-C6 | The leader SHALL persist control-state changes (cluster creation, spec rewrite after restore, tombstones) to all acked peers via `UpdateControl`. | MUST | §3.4 |

### 2.5 Command issuance (FR-CMD)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-CMD1 | `etcd-manager-ctl` SHALL support a `--local` mode that writes a command JSON into a drop directory (`<baseDir>/local-control/commands-inbox/`) on any single node. | MUST | §3.5 |
| FR-CMD2 | The node agent SHALL watch the inbox, persist dropped commands into its `ControlRecord.pending_commands`, and advertise them via `GetInfo`. | MUST | §3.5 |
| FR-CMD3 | `restore-backup best` SHALL be supported: the leader substitutes the top-ranked candidate at execution time. | SHOULD | §3.5, §3.6 |

### 2.6 Restore selection (FR-RS)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-RS1 | Restore SHALL remain operator-triggered by default; the controller SHALL NOT restore automatically. | MUST | §3.6 |
| FR-RS2 | The leader SHALL build the candidate set from all peers' advertised `local_backups`, plus the shared store's backups in hybrid mode. | MUST | §3.6 |
| FR-RS3 | Candidates SHALL be ranked by `etcd_revision` descending, tie-broken by timestamp then name; raft term and wall clock SHALL NOT be primary ranking keys. | MUST | §3.6, §5 |
| FR-RS4 | Before acting, the leader SHALL require the existing `ackedPeerCount >= quorumSize(spec.member_count)` check AND a stable peer set for at least 2 consecutive cycles. | MUST | §3.6 |
| FR-RS5 | If any expected peer is absent, the leader SHALL warn and SHALL require an explicit force flag on the command before proceeding. | MUST | §3.6 |
| FR-RS6 | The leader SHALL refuse a restore command while healthy members >= quorum, unless the command carries a force flag. | MUST | §3.6, §4 (#8) |
| FR-RS7 | The chosen candidate's owner SHALL be an acked, healthy peer (or the shared store in hybrid mode). | MUST | §3.6 |
| FR-RS8 | The leader SHALL log the full consulted-candidate report (peers consulted, candidates, ranking) for every selection. | SHOULD | §3.6 |
| FR-RS9 | An opt-in `--auto-restore` MAY be provided, default off, gated on: cluster known created, 0 members, all peers reporting empty/trashcanned disks, quorum of expected peers present, and a configurable dwell time of continuous 0-member state. | MAY | §3.6 |

### 2.7 Restore execution (FR-RE)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-RE1 | The leader SHALL send `DoRestore{use_local_store: true, backup_name}` to the peer identified by `backup_peer`; that peer restores from its own local store; raft replication distributes the data (unchanged data path). | MUST | §3.7 |
| FR-RE2 | `createNewCluster` SHALL build a deterministic membership proposal; when a restore command with `backup_peer` is pending, that peer SHALL be included in (placed first in) the proposal. | MUST | §3.7 |
| FR-RE3 | The restore step SHALL target the `backup_peer` member specifically and SHALL error if it is not a healthy member (replacing today's arbitrary-healthy-peer selection). | MUST | §3.7 |
| FR-RE4 | The backup's `sha256` SHALL be verified before restore; on mismatch the leader SHALL fall back to the next-ranked candidate (when using `best`) or error (when a specific backup was named). | MUST | §3.3, §4 (#10) |
| FR-RE5 | Restore commands SHALL only be removed (tombstoned) after success; failure or owner death SHALL cause re-selection on the next cycle, relying on `createNewCluster` idempotency. | MUST | §3.7 |

### 2.8 Upgrade path (FR-U)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-U1 | The non-in-place upgrade flow (`stopForUpgrade`) SHALL record `backup_peer` in its self-issued restore command and route it via `UpdateControl` in local mode. | MUST | §3.8 |
| FR-U2 | The object-store visibility busy-wait SHALL be removed in local mode (one gossip cycle suffices). | SHOULD | §3.8 |

### 2.9 Modes and configuration (FR-H)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| FR-H1 | `--backup-store` SHALL become optional; configuring only `--local-backup-dir` selects local-only mode. | MUST | §3.9, §8 |
| FR-H2 | Configuring both SHALL select hybrid mode: local backups every interval plus shared-store backups on a (configurable, slower) cadence; restore candidates merge both sources. | MUST | §3.9 |
| FR-H3 | Hybrid mode SHOULD be the documented recommended posture; local-only mode documentation SHALL state the all-volumes-lost limitation prominently. | MUST | §3.9, §5 |

## 3. Non-functional requirements

### 3.1 Safety (NFR-S)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| NFR-S1 | The system SHALL never auto-bootstrap a new empty cluster when any peer's state implies a cluster previously existed (created-marker OR-merge, FR-C3). | MUST | §3.4, §4 (#4) |
| NFR-S2 | The restore decision SHALL be biased toward inaction under uncertainty (missing peers, unstable peer set, healthy quorum present) — see FR-RS4..RS6. | MUST | §3.6 |
| NFR-S3 | All new leader→node RPCs SHALL be rejected when the leadership token is invalid, preserving stale-leader protection. | MUST | §5 |
| NFR-S4 | Existing split-brain protections (ack-all-healthy-peers leadership, lowest-ID-wins, quorum re-checks in the restore path) SHALL be preserved unchanged. | MUST | §4 (#9), §5 |
| NFR-S5 | Old data SHALL continue to be archived (`data-trashcan/`), never deleted, during cluster recreation. | MUST | §2.4, §3.7 |

### 3.2 Security (NFR-SEC)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| NFR-SEC1 | Nodes SHALL NOT open filesystem paths or storage URLs supplied over RPC in local mode; only locally-configured directories are used. | MUST | §3.1, §5 |
| NFR-SEC2 | Local-mode operation SHALL NOT require object-store credentials on any node. | MUST | §1 |
| NFR-SEC3 | Command-inbox and control files SHALL be validated (path traversal guards on backup names and tokens SHALL continue to apply). | MUST | §2.2, existing `validateBackupName`/`validateSubDirectory` |

### 3.3 Compatibility (NFR-COMP)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| NFR-COMP1 | A mixed-version fleet (old + new etcd-manager) SHALL keep working with shared-store behavior; local mode SHALL be capability-gated on **all** acked peers advertising support. | MUST | §4 (#12), §6 |
| NFR-COMP2 | Migration SHALL be possible without downtime: enable hybrid mode fleet-wide, seed `ControlRecord`s from the shared control store, then optionally drop `--backup-store`. Rollback SHALL be the reverse; etcd-manager SHALL never delete the shared store. | MUST | §6 |
| NFR-COMP3 | Existing `etcd-manager-ctl` store-based subcommands SHALL keep working in hybrid mode. | MUST | §6 |
| NFR-COMP4 | The on-disk backup format SHALL remain the documented portable format (backupstructure.md), extended only by additive meta fields. | MUST | §2.2, §3.2 |

### 3.4 Durability & capacity (NFR-D)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| NFR-D1 | Default backup placement SHALL couple backup survival to etcd-state survival (same persistent volume, remounted across instance replacement). | MUST | §3.1, §5 |
| NFR-D2 | Local retention SHALL bound disk usage (tiering + hard cap); pointing `--local-backup-dir` at a separate volume SHALL be supported. | MUST | §3.1, §4 (#11) |
| NFR-D3 | Documentation SHALL provide volume sizing guidance for backups sharing the data volume. | SHOULD | §4 (#11) |

### 3.5 Observability (NFR-O)

| ID | Requirement | Priority | Source |
|----|-------------|----------|--------|
| NFR-O1 | The leader SHALL log per-cycle backup fan-out results (which members succeeded/failed). | SHOULD | §3.1 |
| NFR-O2 | Restore selection SHALL be fully auditable from logs (candidates, ranking keys, guards evaluated, chosen owner). | SHOULD | §3.6, FR-RS8 |

## 4. Acceptance criteria (verification)

| ID | Criterion |
|----|-----------|
| AC1 | Integration test (local-mode variant of `test/integration/backuprestore`): 3-node cluster, local backups only; stop all etcd-manager processes and wipe process state but keep volumes; restart; assert the controller refuses to bootstrap (NFR-S1) and reports "must issue restore-backup command". |
| AC2 | Continue AC1: drop a restore command into one node's inbox; assert the cluster is recreated and restored from the **highest-revision** backup, and the restored key set matches the last pre-wipe write. |
| AC3 | Unit tests: candidate ranking (revision beats timestamp beats name; term ignored), including a simulated stale-minority peer with inflated term. |
| AC4 | Unit tests: `ControlRecord` merge rules (created = OR incl. backup/data implication; spec LWW; command union-minus-tombstones; tombstone GC). |
| AC5 | Corruption test: flip a byte in the chosen backup; assert sha256 verification fails and fallback to the next candidate succeeds (FR-RE4). |
| AC6 | Dead-owner test: kill the owner of the top-ranked backup between command issuance and restore; assert re-selection picks the next candidate (`best`) or errors (named backup) without data-dir destruction (FR-RE5). |
| AC7 | Mixed-fleet test: one old-binary peer present ⇒ leader stays in shared-store behavior (NFR-COMP1). |
| AC8 | Upgrade test (local-mode variant of `test/integration/upgradedowngrade`): non-in-place version change completes using a local quarantined backup with `backup_peer` routing (FR-U1). |
| AC9 | Duplicate-command test: same restore command dropped on two nodes ⇒ executed once; restore refused while healthy quorum exists unless forced (FR-RS6, FR-C5). |
| AC10 | Retention test: local cleanup honors tiering, hard cap, and newest-backup preservation (FR-B5, FR-B6). |

## 5. Assumptions and dependencies

- A1. Node volumes are persistent and remounted by replacement
  instances (kops volume model); local-only mode's durability claims
  depend on this.
- A2. Operators issuing local commands have host/root access to at
  least one control-plane node.
- A3. etcd v3 only (v2 backup/restore is already unsupported).
- A4. The etcd binary matching a backup's recorded version (or the
  mapped known-good restore version) is installed on the restoring
  node — unchanged from today.
