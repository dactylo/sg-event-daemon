# Operations

This guide is for operators who deploy and run sg-event-daemon. It covers configuration beyond the
first run, health checks, recovery from failures, state storage and backups, Kubernetes, and
capacity. If you have not run the daemon yet, start with the
[README quick start](../README.md#quick-start).

| If you want to...                                  | Start here                                                                        |
| -------------------------------------------------- | --------------------------------------------------------------------------------- |
| Learn the terms this guide uses                    | [How the daemon is organized](#how-the-daemon-is-organized)                       |
| Let plugins write to SG with their own credentials | [Separate reader and writer credentials](#separate-reader-and-writer-credentials) |
| Validate a configuration and start the supervisor  | [Validate and start](#validate-and-start)                                         |
| Choose between eager and strict delivery           | [Choose eager or strict ordering](#choose-eager-or-strict-ordering)               |
| Run plugins in separate processes                  | [Choose workers](#choose-workers)                                                 |
| Monitor health or configure probes                 | [Check status and health](#check-status-and-health)                               |
| Decide how to respond to a status condition        | [Status conditions](#status-conditions)                                           |
| Retry, skip, replay, or reset a group              | [Recover a group or source](#recover-a-group-or-source)                           |
| Back up or restore state                           | [SQLite state and backups](#sqlite-state-and-backups)                             |
| Run on Kubernetes                                  | [Kubernetes](#kubernetes)                                                         |
| Size queues, buffers, and memory                   | [Capacity](#capacity)                                                             |
| Interpret a command's exit code                    | [Exit codes](#exit-codes)                                                         |

## How the daemon is organized

A configuration describes one or more SG sites. Each site has these parts:

- A **source** (`[sites.<site>.sources.<name>]`) polls the site's `EventLogEntry` records with one
  credential. That credential's permissions decide which events the source can see. Groups that use
  the same source share its poller.
- A **group** (`[sites.<site>.groups.<name>]`) is an ordered list of plugins that receive events
  from one source. A group delivers one event at a time, in plugin order, and keeps its own
  position and failure state in SQLite. When a callback fails, its group stops; other groups
  continue.
- A **worker** is a child process that runs groups. By default, all of a site's groups share one
  worker, which also runs the site's sources. You can move groups into named or dedicated workers.
- A group's **generation** is one continuous delivery history. Replaying or resetting a group
  starts a new generation.

The **supervisor** (`sg-event-daemon supervise`) is the foreground process that starts workers,
restarts them after failures, answers status and recovery commands, and serves health probes.

## Configure and start

### Separate reader and writer credentials

The README example reads events and runs callbacks with one credential. For a plugin that writes to
SG, give its callbacks their own credential:

```toml
[sites.studio.credentials.event-reader]
script_name = "event-daemon-reader"
api_key = { env = "SG_EVENT_READER_KEY" }

[sites.studio.credentials.callback-writer]
script_name = "event-daemon-writer"
api_key = { env = "SG_EVENT_WRITER_KEY" }

[sites.studio.sources.primary]
credentials = "event-reader"

[sites.studio.groups.editorial]
source = "primary"
callback_credentials = "callback-writer"
execution = "sync"
ordering = "strict"

[[sites.studio.groups.editorial.plugins]]
id = "shot-status"
path = "plugins/shot_status.py"
config = { project_id = 123 }
```

To try this, keep the `[daemon]` and `[sites.studio]` tables from the README example and replace
its credential, source, and group tables with these. Put both keys in the service environment.

The source credential decides which events the daemon sees. The group's `callback_credentials`
supplies the `sg` handle its callbacks use and can have narrower or different permissions; a plugin
can override it with its own `credentials` key. A group without `callback_credentials` uses its
source's credential. Changing the source credential's permissions in SG can change which events the
daemon sees, even when the configuration does not change.

Relative plugin paths and `state_dir` resolve from the directory of the configuration file after
symlinks are resolved, and each plugin file must exist before validation. Source names in one site
must remain distinct when underscores are replaced with hyphens, because that spelling identifies
their event sockets. Keep non-secret plugin settings in `config`, and environment-backed secrets in
the plugin's `secrets` table; see [credentials and secrets](porting.md#credentials-and-secrets).

### Validate and start

The examples in this guide use shell variables for values you supply: `DAEMON_CONFIG` for the
configuration path, and `SITE`, `SOURCE`, `GROUP`, `WORKER`, `EVENT_ID`, and `REASON` for command
arguments. Validate the configuration offline, then start the supervisor:

```sh
sg-event-daemon validate-config --config "$DAEMON_CONFIG"
sg-event-daemon supervise --config "$DAEMON_CONFIG"
```

`validate-config` checks the configuration, the installed provider, and every plugin's registration
without contacting SG; it contacts SG only when you add `--live`. Validation imports each plugin
and so runs its top-level code: validate only plugins you trust. Environment variables named by
credentials and plugin `secrets` must be set for `validate-config` and `supervise`. Status and
recovery commands read only `state_dir` from the configuration, so they do not need those
variables.

The supervisor runs in the foreground until you stop it with Ctrl-C or SIGTERM, which shuts down
its workers and exits with status 0. `sg-event-daemon --help` lists the commands,
`sg-event-daemon status --help` shows one command's options, and `sg-event-daemon --version` prints
the installed version.

Set `shutdown_deadline_secs` under `[daemon]` to bound each worker's graceful shutdown attempt. It
defaults to 10 seconds. A worker that does not exit within that deadline is force-terminated; the
supervisor waits up to one more deadline to reap it.

On first start, each new group begins after the latest event visible to its source credential and
does not process older history. An ordinary restart resumes each group where it stopped, including
an event in progress, without rerunning callbacks that already completed for that event.

Run one supervisor per state directory; two processes writing the same delivery state would corrupt
it. The supervisor locks `state_dir` at startup and refuses to start while another process holds
the lock, but the lock only excludes processes that see the same filesystem mount. See
[Kubernetes](#kubernetes) for the consequence on shared volumes.

## Choose eager or strict ordering

SG can make an event visible after events with higher IDs. Each group's `ordering` setting decides
how it handles such late events:

- `eager`, the default, delivers each event as soon as the source sees it. It favors latency: a
  late event with a lower ID can arrive after a higher one.
- `strict` holds events for the group's `strict_holdback_secs` (300 seconds by default) so that it
  can deliver them in increasing ID order within a generation, apart from events delivered again
  after a failure. It costs latency and memory, and it cannot deliver an event that SG no longer
  exposes.

Choose `strict` only when callback behavior depends on ID order. Changing the `ordering` of an
existing group stops it with `ordering-changed` until you start a new generation with
[replay or reset](#replay-or-reset-a-group).

### Collect strict acceptance evidence

Before you enable strict ordering, sample each source with its configured credentials and save the
result:

```sh
sg-event-daemon validate-config --config "$DAEMON_CONFIG" --live >acceptance-evidence.toml
sg-event-daemon validate-config --config "$DAEMON_CONFIG" --evidence acceptance-evidence.toml
```

`--live` makes one read-only `EventLogEntry` query per source and prints TOML evidence on standard
output; diagnostics go to standard error. The evidence records each source's identity, the capture
time, and the observed gap rate: the number of missing ID ranges in the sample divided by the
source's `max_open_ranges`. When a supervisor is running, it also records the source's current
stream incarnation.

`--evidence` compares saved evidence with the configuration. For each strict source, validation
warns when evidence is missing or stale, that is, captured for a different site, credential, or
script name, and otherwise reports the observed gap rate. Without `--evidence`, it warns that
evidence is missing for every strict source. These warnings are advisory, and validation still
succeeds. A sample cannot prove future visibility, retention, or permission completeness, so review
the evidence before you enable strict delivery.

## Choose workers

A group's `worker` setting chooses the process that runs it:

| Setting                | Process                                                                                     |
| ---------------------- | ------------------------------------------------------------------------------------------- |
| `worker = "default"`   | The site's default worker, shared with the site's other default groups and its sources.     |
| `worker = "<name>"`    | A named worker defined at `[sites.<site>.workers.<name>]`, shared by every group naming it. |
| `worker = "dedicated"` | A process for this group alone.                                                             |

Sharing a worker changes fault isolation and resource use, not delivery order or guarantees. Use a
named or dedicated worker when plugin code changes process-wide state or needs its own memory
limit, or to keep plugins away from the site's sources: a crash in the default worker also
interrupts event polling for the whole site until the worker restarts.

Each group's `callback_deadline_secs` (120 seconds by default) bounds one setup or callback call.
When a single callback lane overruns its deadline, the supervisor parks that group and replaces the
worker without counting the restart toward its breaker. The event remains in progress and its
callback runs again after an operator unparks the group. Setup overruns, multiple overdue lanes,
and source or operator overruns take the ordinary worker restart path. Validation warns when a
group's deadline is shorter than one SG request with all of its retries, computed from the callback
credential's `timeout_secs`, `max_rpc_attempts`, and `rpc_attempt_interval_secs` (96 seconds with
the defaults).

A write held past its deadline makes its worker stale; the supervisor terminates and replaces it.
If the site worker restarts, its site's named and dedicated workers restart too.

The supervisor restarts a failed worker after a delay that starts at 1 second and doubles up to 60
seconds. If a worker fails more than `restart_limit` times (5 by default) within
`restart_window_secs` (300 seconds by default), its breaker opens and the supervisor stops
restarting it until you [reset the breaker](#reset-a-worker-breaker). After a worker runs for
`healthy_reset_secs` (300 seconds by default) without failing, its failure count and delay start
over. You can set these three keys in a named worker's table; default and dedicated workers use the
defaults.

The supervisor cannot restart itself. A service manager must restart the whole daemon when the
supervisor exits or stops making progress.

## Check status and health

Ask the running supervisor for status, optionally narrowed to one group, or run a single health
check:

```sh
sg-event-daemon status --config "$DAEMON_CONFIG"
sg-event-daemon status --config "$DAEMON_CONFIG" --site "$SITE" --group "$GROUP"
sg-event-daemon status --config "$DAEMON_CONFIG" --check liveness
sg-event-daemon status --config "$DAEMON_CONFIG" --check readiness
```

Status and recovery commands talk to the supervisor through a local control socket. Run them as the
daemon's user: both ends of the socket require the same effective user ID.

`status` prints one line of JSON with `primary_state`, every active condition in `conditions`,
`liveness`, `readiness`, and detailed records for sources, groups, and workers. Group records name
the current event, plugin, callback, and delivery attempt; source and group records include
`phase`, `heartbeat_at`, `heartbeat_age`, `deadline_at`, and `overdue_reason`. Detail is bounded:
`projection_truncated` is `true` when records or late-ID detail were left out, so narrow the query
with `--site` and `--group` to see an affected group.

- **Liveness** means the supervisor is making progress.
- **Readiness** means a fresh, complete health snapshot shows every configured source, worker, and
  unparked group able to accept work. It fails until every expected component has reported.

A failed plugin group fails readiness but not liveness. Alert on readiness rather than letting a
liveness probe restart the daemon in a loop. A parked group does not fail readiness, so monitor
`parked` in status separately, including groups parked by the supervisor after an overdue callback.

`status --check` exits 0 when the check passes and 1 when it fails, so it works as an exec probe
when no HTTP listener is configured. It exits 3 when it cannot reach the supervisor.

Readiness also fails briefly while a group is `catching-up` or a source is `source-degraded` and
retrying SG requests. Allow an alerting window based on observed recovery time, and investigate a
condition that lasts longer.

### Status conditions

`conditions` lists every condition that applies; `primary_state` is the highest-priority one. The
table lists primary states from highest to lowest priority with the usual response. Inspect the
affected source or group before you act.

| Primary state             | Meaning                                                                                        | Usual response                                                                           |
| ------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `worker-breaker-open`     | A worker failed too often, so the supervisor stopped restarting it.                            | Fix the cause, then [reset the breaker](#reset-a-worker-breaker).                        |
| `stale`                   | A component missed its heartbeat or overran a phase deadline, including a write-hold deadline. | Check `overdue_reason` and the logs. The supervisor replaces an overdue worker.          |
| `stream-identity-changed` | A source's site URL, credential name, or script name changed.                                  | [Adopt the new stream](#adopt-a-changed-source).                                         |
| `ordering-changed`        | A group's configured `ordering` differs from the mode its state uses.                          | Park the group, then [replay or reset](#replay-or-reset-a-group) it.                     |
| `event-unavailable`       | A group or source cannot fetch or encode an event it needs.                                    | Check the affected record; `retry` refetches an event a group is waiting for.            |
| `history-truncated`       | SG retention passed required history, possibly including the in-flight event.                  | Park, then [replay or reset](#replay-or-reset-a-group) with audited retirement.          |
| `ordering-anomaly`        | A strict group received an event with a lower ID than one it already delivered.                | Diagnose, then [acknowledge it](#acknowledge-an-ordering-anomaly).                       |
| `capacity-exhausted`      | A configured queue, buffer, or late-event limit was reached.                                   | Review the affected source's [limits](#capacity).                                        |
| `callback-failed`         | Plugin setup or a callback raised an exception.                                                | Fix the cause, then [retry, skip, or restart](#retry-skip-or-restart-the-current-event). |
| `source-degraded`         | A source is retrying failed SG requests.                                                       | Usually transient; investigate if it persists.                                           |
| `catching-up`             | A group is reading a backlog directly from SG.                                                 | Usually transient; wait.                                                                 |
| `parked`                  | An operator or the supervisor parked a group.                                                  | Inspect the group, then [unpark](#park-and-unpark-a-group) it when ready.                |
| `idle`                    | A group is waiting for events, and nothing needs attention.                                    | None.                                                                                    |
| `healthy`                 | No condition applies.                                                                          | None.                                                                                    |

These group conditions appear in `conditions` but never become the primary state. Unless the group
is parked, they also fail readiness:

| Condition         | Meaning                                                                   | Usual response                                                       |
| ----------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `shape-mismatch`  | The group's callback registration changed while an event was in progress. | Restore the previous registration, or run `restart-event`.           |
| `required-event`  | The group needs an exact event that is currently unavailable.             | Inspect `event-unavailable`, then `retry` to refetch that event.     |
| `replay-required` | The group must be replayed before it can continue.                        | Park the group, then [replay or reset](#replay-or-reset-a-group) it. |
| `orphaned`        | The group was removed from the configuration and later added back.        | [Replay or reset](#replay-or-reset-a-group) it, then unpark it.      |

### HTTP probes

Set `probe_port` to enable the HTTP listener; without it, the daemon opens no listener:

```toml
[daemon]
probe_bind = "127.0.0.1"
probe_port = 8080
```

The listener answers `GET` and `HEAD` requests for `/livez` and `/readyz` with `200` or `503` and
no status details. It does not authenticate clients or terminate TLS. `probe_bind` accepts an IPv4
or IPv6 address literal and defaults to `0.0.0.0`, every IPv4 interface, so bind it to a trusted
interface and apply network policy. Under a tight CPU quota, prefer HTTP probes: an exec probe
starts a Python interpreter for every check.

The listener serves up to 16 concurrent connections, each with a one-second total read and write
budget, and immediately closes extra connections. One idle connection does not delay other admitted
peers, but 16 idle connections exhaust the capacity. Keep network policy restrictions in place.

`/livez` fails when the supervisor's progress heartbeat is stale. `/readyz` fails when the complete
health snapshot is stale or not ready. A listener that still answers does not by itself prove the
supervisor is making progress, and if the process cannot schedule the listener, clients see a
timeout or connection failure. A provider error usually leaves liveness healthy and fails readiness
while the source retries; worker repair and an open breaker also affect only readiness. A terminal
halt fails both.

### Probe timing

Choose probe timeouts and failure windows from measured scheduling delay and your restart target.
The supervisor derives its staleness limits from two `[daemon]` settings: `health_interval_secs`
(`H`, 2 seconds by default) and `supervisor_poll_interval_secs` (`P`, 1 second by default).

```text
control_progress_budget = max(P, 2 * H, 5 seconds)
supervisor_stale_after = 2 * control_progress_budget
readiness_snapshot_stale_after = 2 * max(H, P)
```

The fixed 5 seconds is the control request timeout. A write lock has its own bounded acquisition
budget; SQLite waits for a write transaction only for the budget remaining at that point. With the
defaults, liveness fails after 10 seconds without supervisor progress, and readiness fails once the
latest health snapshot is 4 seconds old. Set probe timeouts above the measured scheduling delay,
choose a consecutive-failure window that meets your restart target, and leave startup time for
workers and the first complete health report.

## Recover a group or source

Recovery commands run through the running supervisor. They do not edit SQLite files directly, and
they refuse a change when the current state does not match what the command expects. Before you
act, inspect `status` and any failed callback.

Every recovery command except `retry` requires `--reason`. The daemon records the command, your
reason, and your local user name, or your numeric user ID when you have no passwd entry, in an
audit record. Access control always uses the numeric user ID.

Recovery delivers at least once: `retry`, `restart-event`, and `replay` can repeat side effects
that already happened, so plugins must tolerate repetition. See
[idempotent callbacks](porting.md#callbacks-must-tolerate-repeats).

### Retry, skip, or restart the current event

Retry the failed setup or callback, or refetch an event the group is waiting for:

```sh
sg-event-daemon retry --config "$DAEMON_CONFIG" --site "$SITE" --group "$GROUP"
```

`retry` reruns the plugin code already loaded in the worker. To retry with fixed plugin code,
deploy the fix and restart the daemon first. A failed callback stays recorded across restarts, so
the group waits for your `retry`. Fixing code does not change a callback's registration, but
renaming or reordering callbacks does; see `shape-mismatch` below.

Skip only the failed callback, or restart the whole in-flight event:

```sh
sg-event-daemon skip-callback --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
sg-event-daemon restart-event --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
```

`skip-callback` marks the failed callback as skipped and continues with the event's remaining
callbacks; you accept that its effect never happens. `restart-event` runs the event again under the
current callback registration and can repeat callbacks that already completed for it.

If the group's callback registration changed while the event was in progress, the group reports
`shape-mismatch` and refuses `skip-callback`. Restore the previous registration, or use
`restart-event` to run the event under the new one.

### Park and unpark a group

```sh
sg-event-daemon park --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
sg-event-daemon unpark --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
```

Parking stops a group's callbacks but keeps its position and state; unparking resumes delivery. A
parked group does not fail readiness.

### Replay or reset a group

Replay a whole group from an event ID, or discard its outstanding history and move it to the latest
event:

```sh
sg-event-daemon replay --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --from-event-id "$EVENT_ID" --reason "$REASON"
sg-event-daemon reset-group-to-latest --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
```

Both commands change the group's position, so they require a parked group with no event in
progress, except for the audited recovery cases described below. Both start a new generation. The
group stays parked afterward: inspect its status, then unpark it.

Command arguments, including `--reason`, identify a retry. Repeating the most recently completed
command with identical arguments returns its recorded result without starting another generation.
Reusing those arguments after an intervening replay or reset is rejected before changing the group.
Give a new operation a distinct reason.

`replay` starts with `--from-event-id`, inclusive, and replays every plugin in the group. Callbacks
see `is_operator_replay` set to true while replay is catching up; after the group rejoins live
delivery, they see false. Replay returns only events SG exposes at replay time; retention,
permissions, and upstream replica behavior can leave gaps. It is at-least-once recovery, not an
exact reconstruction of history.

`reset-group-to-latest` discards the group's outstanding delivery history. Events between its old
position and the latest event are never delivered to it.

A replay cannot start below SG's visible retention floor. When the floor has passed the point the
group must replay from (`history-truncated`), a replay that starts at or after the floor also needs
`--acknowledge-truncation`, which accepts that the events below the floor are lost. Otherwise,
reset the group to the latest event.

When the persisted `history-truncated` floor has passed the in-flight event, an acknowledged replay
from at or above the floor, or a reset, can retire that event. This requires the active group and
source identity to match; it does not apply to orphaned groups or changed streams. Replay must
still pass the source-frontier and retained replay-marker checks. Every undelivered event below the
floor may be lost; reset also discards the remaining visible history. Before acting, inspect
`current_event_id` in status. The group's audit records the retired event, complete prior state,
operator, and reason under `retire-truncated-event`.

A parked active group halted by collapsed obligations can also replay an in-flight event when its
current replay marker and replay position cover that event, including the acknowledged
retention-floor override above. That existing exception records `replay-collapsed-event`.

### Adopt a changed source

The daemon identifies a source's event stream by the site URL, the source's credential name, and
that credential's script name. When one of these changes, the source stops with
`stream-identity-changed`. A permission change in SG is not detected as an identity change, and
this command cannot adopt it.

Inspect the new visible stream before adopting it. Adoption parks every group that uses the source
and discards the old stream's position and pending work; it does not carry a cursor over. Then
replay or reset each of those groups, and unpark it:

```sh
sg-event-daemon adopt-source-stream --config "$DAEMON_CONFIG" \
    --site "$SITE" --source "$SOURCE" --reason "$REASON"
sg-event-daemon reset-group-to-latest --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
```

### Acknowledge an ordering anomaly

A strict group stops with `ordering-anomaly` when an event arrives with a lower ID than one it has
already delivered. Diagnose the cause, then record an audited acknowledgement, which clears the
stop:

```sh
sg-event-daemon acknowledge-ordering-anomaly --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
```

Acknowledging accepts that the late event is skipped: its callbacks never run, and the group
resumes with later events. If the group must process the late event, park the group and
[replay](#replay-or-reset-a-group) from that event's ID instead. Replay also clears the stop, but
it delivers the events after the late one again.

### Reset a worker breaker

When a worker's breaker is open, fix the cause of its failures, then let the supervisor restart it:

```sh
sg-event-daemon reset-worker-breaker --config "$DAEMON_CONFIG" \
    --site "$SITE" --worker "$WORKER" --reason "$REASON"
```

`--worker` takes the worker's ID from the `worker_id` field of `status`, not a group name. A worker
ID is a compact JSON array: `["site","studio"]` for site `studio`'s default worker,
`["named","studio","editorial"]` for its named worker `editorial`, and
`["dedicated","studio","<group>"]` for a dedicated worker. Quote it for the shell, for example
`--worker '["named","studio","editorial"]'`.

### Remove or rename a group

Removing a group from the configuration retires its current generation. The daemon retains the
group's state inside its source database, but events that arrive while the group is absent are not
delivered to it. If you add the group back under the same name, it starts parked and needs a replay
or reset before you unpark it. Renaming a group creates a new group that starts at the latest
event; the old group's state does not move with it.

Keep a source and its site configured while a retained group record references that source and its
source database exists. Removing either stops startup with
`durable source is no longer configured`; restoring the configuration lets startup proceed. A
configured group's `source` cannot be changed under the same group name: startup rejects it with
`configured group source changed without adoption`. To use a different source, give the group a new
name and plan its starting position as for any new group. `adopt-source-stream` handles a changed
stream identity for the same source; it does not move a group between sources.

### Audit records

Audit records accumulate. Monitor `audit_count` and `database_sizes` in status, along with disk
space. `audit_count` includes supervisor, source, and group audit records without double counting.
`database_sizes` lists `supervisor.db` and one file per source; retained groups count toward their
source file's size.

## SQLite state and backups

### Storage

Keep `state_dir` on persistent local storage, owned by the daemon's user with mode `0700`. Startup
rejects a state directory that is a symlink, has the wrong owner or mode, or is already in use by
another supervisor. Its parent must be owned by root or the daemon's user, be a directory that can
be opened securely, and not be writable by other users unless it has the sticky bit set.

The directory contains `supervisor.db` and one SQLite database per source. Each source file holds
that source's state and all its groups' state, including orphaned groups. Keep every process that
uses these files on the same host. Do not put state on EFS, NFS, SMB, or another network
filesystem: SQLite's write-ahead log (WAL) and the required locks need a local filesystem. Each
worker process checkpoints the source files it opens. Its checkpointer stops the worker after five
checkpoint intervals without a complete or reset-observed attempt, or when it observes committed
WAL frames beyond the growth tripwire. These limits do not cap the WAL file's physical size;
monitor its actual size, free space, and checkpoint health. A corrupt source file or wedged writer
stops every group on that source. A site-worker restart also restarts that site's named and
dedicated workers.

### Durability

`sqlite_synchronous` in `[daemon]` trades write cost against how much recent progress survives a
power loss:

- `"normal"`, the default, can roll back the most recent commits after node or power loss. The
  group then delivers those events again, which at-least-once delivery allows. A state change is
  never partly applied.
- `"full"` syncs every ordinary commit, which narrows that replay window at a higher I/O cost.

Neither mode makes plugin side effects exactly once. On macOS, SQLite does not issue `F_FULLFSYNC`
unless `PRAGMA fullfsync=ON`; the daemon does not set it. Thus `"full"` protects against process
and kernel failure there, but does not claim power-loss durability with a write-caching drive.
macOS is supported for development and foreground supervision, not production power-loss
durability.

### Socket paths

The supervisor, its workers, and operator commands communicate through Unix sockets in `state_dir`.
A socket path can be at most 107 bytes on Linux or 103 bytes on macOS. When a path would be longer,
the daemon uses a short hashed name under a private runtime directory: first `XDG_RUNTIME_DIR`,
then the process's temporary directory, usually set by `TMPDIR`. That directory must already exist,
be owned by the daemon's user with mode `0700`, and not be a symlink, so a shared `/tmp` does not
qualify. If no candidate is safe and short enough, startup and control commands reject the path.

To fix that, use a shorter `state_dir`, or create a short private directory and set
`XDG_RUNTIME_DIR` to it for both the daemon and operator commands. Restart the daemon after you
change the runtime directory, because existing sockets do not move. Persistent state always stays
in `state_dir`.

### Back up and restore

1. Prefer a clean supervisor stop, then snapshot the complete `state_dir`, including
   `supervisor.db` and every source file.
2. For an online backup, use a storage snapshot coordinated with a checkpoint that captures the
   complete `state_dir`, including every source file, `supervisor.db`, and WAL data needed for
   recovery, at one consistent restore point. Independent SQLite backup API copies of individual
   files do not meet this requirement; using that API requires coordination across all files to
   establish the same restore point. Copying only a database file can miss changes still in its
   WAL.
3. With the daemon stopped, restore the whole `state_dir` from one snapshot. Preserve ownership and
   modes, and test each restore in a separate state directory. A restore repeats deliveries after
   the snapshot point and loses later operator decisions, which must be repeated.

Restoring a single source file without its matching `supervisor.db` is unsupported; startup can
fail closed when its generations trail the supervisor inventory. A single group cannot be restored
alone because its state is inside its source file.

On corruption, a full disk, a database schema newer than the installed version supports, or
repeated checkpoint failure, preserve the files and logs. Free space or restore a verified backup
before you use audited recovery, particularly when SG no longer holds the event history a group
needs. The daemon does not delete and recreate damaged state.

## Kubernetes

Run exactly one pod with one supervisor and all its workers. Use an EBS volume through the EBS CSI
driver (gp3 by default), formatted ext4 or xfs, with one PersistentVolumeClaim mounted
`ReadWriteOncePod`. A StatefulSet with `replicas: 1` and a `volumeClaimTemplate` is recommended;
its default `RollingUpdate` replaces the old pod after termination. `OnDelete` is also acceptable.
A Deployment requires `replicas: 1` and `strategy: Recreate`; its replacement after an eviction can
still start before the old pod is confirmed stopped. If `ReadWriteOncePod` is unavailable,
`ReadWriteOnce` is best effort: it does not fence a forced detach and second attachment. Do not use
`ReadWriteMany`, EFS, or a `hostPath` shared between nodes. Keep private event and control sockets
local to the pod, and run no workers in another pod or container.

Mount the volume above `state_dir`, for example at `/var/lib/sg-event-daemon` with
`state_dir = "/var/lib/sg-event-daemon/state"`. Use a fixed `runAsUser` so directory ownership
survives rescheduling. The supervisor stops workers sequentially, so allow about the number of
workers times twice `shutdown_deadline_secs` for `terminationGracePeriodSeconds`, plus time for
other shutdown work. For EBS, use `volumeBindingMode: WaitForFirstConsumer` so the pod and volume
bind in the same availability zone. A zone outage leaves the daemon unavailable until that zone
returns or a whole-`state_dir` snapshot is restored to a new volume in another zone.

If a node becomes unreachable, wait for it to return or verify that it is shut down: its instance
must be stopped or terminated, not merely unreachable or restarting. Only after that verification,
apply the cluster's `node.kubernetes.io/out-of-service` taint to let Kubernetes delete the pod and
detach the volume. The replacement pod can then attach it. Do not force-delete the pod or detach
the volume while the old pod may still run. A storage lock on the replacement node does not prove
the old node has stopped; forced detach is not fencing. Recovery has no guaranteed time bound.

On EBS, use the default `"normal"` synchronization unless storage-gate evidence on the actual
volume supports `"full"`; fsync latency can materially limit a single writer's throughput.

For an enabled HTTP listener, start with probes like these, then tune them with the
[probe timing](#probe-timing) guidance:

```yaml
startupProbe:
  httpGet:
    path: /livez
    port: 8080
  periodSeconds: 5
  timeoutSeconds: 2
  failureThreshold: 18

livenessProbe:
  httpGet:
    path: /livez
    port: 8080
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  periodSeconds: 5
  timeoutSeconds: 2
  failureThreshold: 1
```

## Capacity

Size worker memory for event pages fetched from SG, source and group queues, strict buffers, one
in-flight event per group, IPC frames, decoded Python objects, and any copies your plugins keep.
Event contents stay in bounded memory and private IPC frames; SQLite stores only control state.

Tune count and byte limits together: `queue_max_events` with `queue_max_bytes`, `strict_max_events`
with `strict_max_bytes`, and `batch_size` with `max_event_bytes` and `commit_batch_max_bytes`.
Every buffer that holds complete frames must fit one maximum-size event, and validation rejects
limits that cannot. Startup also checks that each worker's health report fits
`max_ipc_frame_bytes`.

| Setting                  | Table                             | Default |
| ------------------------ | --------------------------------- | ------- |
| `queue_max_events`       | `[sites.<site>.sources.<source>]` | 1,000   |
| `queue_max_bytes`        | `[sites.<site>.sources.<source>]` | 64 MiB  |
| `strict_max_events`      | `[sites.<site>.sources.<source>]` | 20,000  |
| `strict_max_bytes`       | `[sites.<site>.sources.<source>]` | 256 MiB |
| `batch_size`             | `[sites.<site>.sources.<source>]` | 500     |
| `max_event_bytes`        | `[sites.<site>.sources.<source>]` | 4 MiB   |
| `commit_batch_max_bytes` | `[sites.<site>.sources.<source>]` | 64 MiB  |
| `max_ipc_frame_bytes`    | `[daemon]`                        | 8 MiB   |

Tracking late events and ID gaps uses these source limits: `max_open_ranges` (100 by default),
`max_range_query_pages` (100), `max_pending_group_obligations` (1,300), `max_source_late_ids`
(50,000), and `max_late_ids_per_group` (50,000). `health_interval_secs` and
`supervisor_poll_interval_secs` in `[daemon]` set the health and progress intervals described under
[probe timing](#probe-timing).

A strict group catches up only when it delivers faster than new events arrive. Estimate its
recovery time as one holdback plus the backlog divided by the difference between those rates. When
a limit is reached, the daemon stops or detaches the affected group before it advances; it never
silently skips an event.

Set limits for a particular site from what you observe: event gaps, status fields such as
`outstanding_late_id_count` and `queue_byte_count`, database growth, memory use, and callback
throughput. The numeric distance between `EventLogEntry` IDs is not a memory estimate. Measure on
the storage and CPU budget that will run the daemon.

## Exit codes

`sg-event-daemon --help` and `sg-event-daemon --version` print to standard output and exit with
status 0. Every `sg-event-daemon` command uses these exit codes:

| Code | Meaning                                                                                                                                                                   |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0    | Success. For `status --check`, the check passed. A supervisor stopped with Ctrl-C or SIGTERM exits 0 after a clean shutdown.                                              |
| 1    | The health check failed, a recovery command was refused or failed, or a runtime error occurred, including a failed or invalid SG request during `validate-config --live`. |
| 2    | The command line, configuration, acceptance evidence, provider, or plugin checked by `validate-config` is invalid, or a configuration or evidence file cannot be read.    |
| 3    | The supervisor's control socket is unavailable, or its reply could not be understood.                                                                                     |
| 130  | Ctrl-C interrupted the command before it finished.                                                                                                                        |

A failed command prints one line on standard error that starts with `error:`. A usage error prints
the command's usage line first, and a failed `status --check` prints only its JSON result. If a
reader closes standard output early, for example when you pipe `status` output into `head`, the
command stops writing and keeps the exit code of the work it completed.

Under `supervise`, a plugin import or registration failure is a worker failure. The supervisor
restarts that worker with backoff until its breaker opens; it does not exit with code 2 for that
plugin failure.
