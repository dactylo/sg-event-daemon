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
| Look up a configuration key                        | [Configuration reference](#configuration-reference)                               |
| Choose between eager and strict delivery           | [Choose eager or strict ordering](#choose-eager-or-strict-ordering)               |
| Decide how long to wait for late events            | [Set the late-event window](#set-the-late-event-window)                           |
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

[[sites.studio.groups.editorial.plugins]]
id = "shot-status"
path = "plugins/shot_status.py"
config = { project_id = 123 }
```

To try this, save the plugin from [Before and after](porting.md#before-and-after) as
`plugins/shot_status.py`. Keep the `[daemon]` and `[sites.studio]` tables from the README example,
replace its credential, source, and group tables with these, and put both keys in the service
environment.

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
arguments. Set the environment variables named by credentials and plugin `secrets` before
validation or startup. Validation imports each plugin and runs its top-level code: use only plugins
you trust.

Validate the configuration offline, then start the supervisor:

```sh
sg-event-daemon validate-config --config "$DAEMON_CONFIG"
sg-event-daemon supervise --config "$DAEMON_CONFIG"
```

`validate-config` checks the configuration, the installed provider, and every plugin's registration
without contacting SG; add `--live` to collect
[source evidence](#collect-strict-acceptance-evidence) from SG. Successful offline validation
prints a JSON result with `validated` set to `true`. Status and recovery commands read only
`state_dir` from the configuration, so they do not need credential or secret variables.

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

| Mode              | Delivery order                                                                                  | Wait                                                                                                                                                                                        |
| ----------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `eager` (default) | Delivers events as the source sees them. A late event follows any higher IDs already delivered. | A connected group receives a late event within a poll or two when the recheck budget covers the open gaps.                                                                                  |
| `strict`          | Delivers increasing IDs within a generation, apart from repeats after failure.                  | At least `strict_holdback_secs` (default 2 seconds). Behind a missing lower ID, waits for the longer of `late_event_window_secs` and `strict_holdback_secs`, plus final scan and close-out. |

Strict ordering costs latency and memory. Neither mode can deliver an event SG no longer exposes.

Choose `strict` only when callback behavior depends on ID order. Changing the `ordering` of an
existing group stops it with `ordering-changed` until you start a new generation with
[replay or reset](#replay-or-reset-a-group).

### Set the late-event window

When the source sees a gap in event IDs, it rechecks the missing IDs at most once per
`fetch_interval_secs` (1 second by default). A gap closes as soon as every ID appears. The source's
`late_event_window_secs` (300 seconds by default) is how long it waits for IDs that never appear
before a final scan; such a gap closes only after its deadline and that complete scan:

```toml
[sites.studio.sources.primary]
credentials = "event-reader"
late_event_window_secs = 600
```

Only time with successful SG queries counts toward the window, so an SG outage or a daemon restart
does not use it up. A connected eager group receives late events as they appear and fetches only
ones it missed after the deadline; a disconnected eager group fetches them as soon as the gap
closes. A strict group fetches them in ID order during close-out after the gap closes and its hold
has passed. An event that SG makes visible after the source gives up its ID is not guaranteed to
reach any group. If a group must process it, [replay](#replay-or-reset-a-group) the group from its
ID. Source logs include these lines (with concrete values in place of braces):

```text
late events in IDs {low}-{high} all appeared; gap closed after {secs}s
gave up on {count} event IDs after {secs}s: {runs}
```

An event appears late when, for example, a large batch commits after smaller changes that started
later. Some gaps never fill, for example when SG uses an ID for a change that never completes. A
source credential that cannot read every event also sees a permanent gap for each event it cannot
see, so give source credentials SG's API Admin permission role. A longer window catches later
events, at a cost:

- A strict group holds events above a gap that never fills until the gap closes and its close-out
  finishes. A connected eager group keeps receiving late events during the window.
- A gap that never fills occupies a range slot longer. The source pauses new gap admission at a
  limit while it continues to work on existing gaps. Size `max_open_ranges` with the window and the
  gap rate that [strict acceptance evidence](#collect-strict-acceptance-evidence) reports. See the
  [configuration reference](#configuration-reference) for the coupled
  `daemon.max_group_obligations` limit. The default range and pending-obligation limits cover bulk
  work at the target scale for an API Admin source whose gaps fill within seconds. For gaps that
  never fill, size those limits from your source's evidence and window; each range remains open
  until its deadline and final scan.

A changed window takes effect after a restart and applies to gaps that open afterward; open gaps
keep their original deadlines.

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

Groups sharing a worker also share its CPU. One worker performs about one core of Python work, so a
group can fall behind under bulk load when another group or a source keeps that worker busy. Place
a group that needs bulk throughput or low delay in a dedicated or named worker.

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

`--check` reports global health. Use `status` without `--check` when narrowing detailed records
with `--site` or `--group`.

`status` prints one line of JSON with `primary_state`, every active condition in `conditions`,
`liveness`, `readiness`, and detailed records for sources, groups, and workers. Group records name
the current event, plugin, callback, and delivery attempt; source and group records include
`phase`, `heartbeat_at`, `heartbeat_age`, `deadline_at`, and `overdue_reason`. Detail is bounded:
`projection_truncated` is `true` when records or late-ID detail were left out, so narrow the query
with `--site` and `--group` to see an affected group.

Workers with an open breaker remain in `workers` as stopped-worker records containing `site`,
`worker_id`, `breaker_open: true`, and `breaker_reason`. Use that `worker_id` with
`reset-worker-breaker`. Stopped records have no heartbeat or live-thread fields; running worker
records contain live health fields and no breaker or `source_outages` fields.

Group records include `replay_required_from`, the latest safe replay start that covers retained
replay evidence, and `history_truncated_floor`, the earliest visible event after history loss. A
null value means that bound is absent. Replay refusals include the bound that rejected the
requested event ID. See [Replay or reset a group](#replay-or-reset-a-group) for truncation
acknowledgement.

In group status and health, `outstanding_late_id_count` and `outstanding_late_ids` refresh at most
once per `health_interval_secs` on the existing health grid. Their displayed values can lag by one
interval plus the group owner's delay before its next loop pass, such as time spent in a callback.
Admission and capacity decisions use current operational state, not these display values.

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

### HTTP probes

While a recovery command runs, `status` and `status --check` can exit 3 after about 5 seconds
without an answer. HTTP probes remain available; prefer them for service-manager health checks.

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

### Status conditions

`conditions` lists every condition that applies; `primary_state` is the highest-priority one. The
table lists primary states from highest to lowest priority with the usual response. Inspect the
affected source or group before you act.

| Primary state             | Meaning                                                                                                                                                                                    | Usual response                                                                                                                                                                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `worker-breaker-open`     | A worker failed too often, so the supervisor stopped restarting it.                                                                                                                        | Fix the cause, then [reset the breaker](#reset-a-worker-breaker).                                                                                                                                                                                                    |
| `stale`                   | A component missed its heartbeat, overran a phase deadline (including a write-hold deadline), or has not sent its first health report.                                                     | Check `overdue_reason` and the logs. The supervisor replaces a worker after five health intervals without input. Input already waiting after a supervisor delay is read before that decision. For a new group still in its first query, inspect its source's status. |
| `stream-identity-changed` | A source's site URL, credential name, or script name changed.                                                                                                                              | [Adopt the new stream](#adopt-a-changed-source).                                                                                                                                                                                                                     |
| `ordering-changed`        | A group's configured `ordering` differs from the mode its state uses.                                                                                                                      | Park the group, then [replay or reset](#replay-or-reset-a-group) it.                                                                                                                                                                                                 |
| `event-unavailable`       | A group cannot fetch an event it needs, or a source halted on an event it cannot encode or that exceeds a size limit.                                                                      | For a group, inspect its record; `retry` refetches the event. For a source, fix the cause and restart the daemon; see below.                                                                                                                                         |
| `history-truncated`       | SG retention passed required history, possibly including the in-flight event.                                                                                                              | Park, then [replay or reset](#replay-or-reset-a-group) with audited retirement.                                                                                                                                                                                      |
| `ordering-anomaly`        | A strict group received an event with a lower ID than one it already delivered.                                                                                                            | Diagnose, then [acknowledge it](#acknowledge-an-ordering-anomaly).                                                                                                                                                                                                   |
| `capacity-exhausted`      | A source paused gap admission, a deadline scan hit its page cap, or a group hit a limit.                                                                                                   | Check [when a source pauses](#when-a-source-pauses) and [capacity](#capacity).                                                                                                                                                                                       |
| `callback-failed`         | Plugin setup or a callback raised an exception.                                                                                                                                            | Fix the cause, then [retry, skip, or restart](#retry-skip-or-restart-the-current-event).                                                                                                                                                                             |
| `write-lock-stalled`      | Writes to a source file are waiting: a connection outside the daemon holds its write lock, or daemon writers are queued. Pending writes to `supervisor.db` can also report this condition. | End an outside connection if one is found. Source-file lock warnings name the level and wait; WAL pauses and pending `supervisor.db` writes have separate warnings.                                                                                                  |
| `checkpoint-stalled`      | A source file or `supervisor.db` crossed a checkpoint bound: a connection outside the daemon may hold the file, or load may keep WAL reuse busy.                                           | End an outside connection if one is found; otherwise reduce load or restart the daemon. If the same source also reports `write-lock-stalled`, its writes are paused until WAL reuse succeeds: find the outside reader.                                               |
| `source-degraded`         | A source is retrying failed SG requests.                                                                                                                                                   | Usually transient; investigate if it persists.                                                                                                                                                                                                                       |
| `catching-up`             | A group is reading a backlog directly from SG.                                                                                                                                             | Usually transient; wait.                                                                                                                                                                                                                                             |
| `parked`                  | An operator or the supervisor parked a group.                                                                                                                                              | Inspect the group, then [unpark](#park-and-unpark-a-group) it when ready.                                                                                                                                                                                            |
| `idle`                    | A group is waiting for events, and nothing needs attention.                                                                                                                                | None.                                                                                                                                                                                                                                                                |
| `healthy`                 | No condition applies.                                                                                                                                                                      | None.                                                                                                                                                                                                                                                                |

For a source, `encoder-failed`, `event-too-large`, and `frame-too-large` appear as
`event-unavailable`. The source stays halted with its worker up until you fix the plugin or data
for `encoder-failed`, or raise the applicable `max_event_bytes`, `max_ipc_frame_bytes`, or
`commit_batch_max_bytes` limit for an oversized event, then restart the daemon. A
`commit-batch-capacity-exhausted` source halt appears as `frame-too-large`. Groups can continue
delivering events that the source already committed.

These group conditions appear in `conditions` but never become the primary state. Unless the group
is parked, they also fail readiness:

| Condition         | Meaning                                                                   | Usual response                                                       |
| ----------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `shape-mismatch`  | The group's callback registration changed while an event was in progress. | Restore the previous registration, or run `restart-event`.           |
| `required-event`  | The group needs an exact event that is currently unavailable.             | Inspect `event-unavailable`, then `retry` to refetch that event.     |
| `replay-required` | The group must be replayed before it can continue.                        | Park the group, then [replay or reset](#replay-or-reset-a-group) it. |
| `orphaned`        | The group was removed from the configuration and later added back.        | [Replay or reset](#replay-or-reset-a-group) it, then unpark it.      |

If a new group is absent from status while its source is still making its first query, status can
remain `stale` and not ready until the group sends its first health report. The source's row shows
its phase, retry, and last error. The group logs
`could not read the latest event from SG to start the group: {error}; retrying in {secs}s` when SG
cannot answer its initial query. Check SG availability and the source's retry details; the group
retries without failing its worker.

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

After startup, a worker that sends nothing for five health intervals is replaced. If the supervisor
was delayed and worker input is already waiting, it reads that input before deciding whether
silence requires replacement. Waiting input alone does not make a worker healthy or clear its
restart history, and it does not defer other confirmed failures. These timeouts depend on the
supervisor being scheduled; they cannot force recovery while it cannot run.

## Recover a group or source

Recovery commands run through the running supervisor. They do not edit SQLite files directly, and
they refuse a change when the current state does not match what the command expects. Before you
act, inspect `status` and any failed callback.

Every recovery command except `retry` requires `--reason`. The daemon records the command, your
reason, and your local user name, or your numeric user ID when you have no passwd entry, in an
audit record. Access control always uses the numeric user ID. Do not put secrets in `--reason`;
text such as `token=` or `password=` is rejected before the command is sent.

Recovery delivers at least once: `retry`, `restart-event`, and `replay` can repeat side effects
that already happened, so plugins must tolerate repetition. See
[idempotent callbacks](porting.md#callbacks-must-tolerate-repeats).

| Situation                              | Action                                                                                                       |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Setup or callback failed               | [Retry, skip, or restart](#retry-skip-or-restart-the-current-event).                                         |
| You need to stop a group temporarily   | [Park it](#park-and-unpark-a-group).                                                                         |
| You need a different starting position | [Replay or reset](#replay-or-reset-a-group).                                                                 |
| Source identity changed                | [Adopt the stream](#adopt-a-changed-source).                                                                 |
| Gap admission or close-out is delayed  | Check [source pauses](#when-a-source-pauses) or [missing close-out events](#when-close-out-misses-an-event). |

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

Parking stops a group's callbacks but keeps its position and state; unparking resumes delivery. A
parked group does not fail readiness.

```sh
sg-event-daemon park --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
sg-event-daemon unpark --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
```

### Replay or reset a group

Replay a whole group from an event ID, or reset it to the latest event. Replay can repeat side
effects. Reset discards outstanding history: events between the old position and the latest event
will never be delivered to this group.

Both commands require a parked group with no event in progress, except for the audited recovery
cases below. They start a new generation and leave the group parked. Afterward, inspect status
before you unpark it.

```sh
sg-event-daemon replay --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --from-event-id "$EVENT_ID" --reason "$REASON"
sg-event-daemon reset-group-to-latest --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
```

Command arguments, including `--reason`, identify a retry. Repeating the most recently completed
command with identical arguments returns its recorded result without starting another generation.
Reusing those arguments after an intervening replay or reset is rejected before changing the group.
Give a new operation a distinct reason.

`replay` starts with `--from-event-id`, inclusive, and replays every plugin in the group. Callbacks
see `is_operator_replay` set to true while replay is catching up; after the group rejoins live
delivery, they see false. Replay returns only events SG exposes at replay time; retention,
permissions, and upstream replica behavior can leave gaps. It is at-least-once recovery, not an
exact reconstruction of history.

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
already delivered. Acknowledging skips that late event: its callbacks never run. If the group must
process it, park the group and [replay](#replay-or-reset-a-group) from that ID instead; replay
clears the stop and delivers later events again.

To accept the skip, diagnose the cause, then record an audited acknowledgement:

```sh
sg-event-daemon acknowledge-ordering-anomaly --config "$DAEMON_CONFIG" \
    --site "$SITE" --group "$GROUP" --reason "$REASON"
```

The command clears the stop and resumes delivery with later events.

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
group's state inside its source database. An interrupted event is retired into the group's replay
evidence without advancing its cursor, so removing a failed group does not block other groups from
starting. Events that arrive while the group is absent are not delivered to it. If you add the
group back under the same name, it starts parked and needs a replay or reset before you unpark it.
Renaming a group creates a new group that starts at the latest event; the old group's state does
not move with it.

Keep a source and its site configured while a retained group record references that source and its
source database exists. Removing either stops startup with
`durable source is no longer configured`; restoring the configuration lets startup proceed. A
configured group's `source` cannot be changed under the same group name: startup rejects it with
`configured group source changed without adoption`. To use a different source, give the group a new
name and plan its starting position as for any new group. `adopt-source-stream` handles a changed
stream identity for the same source; it does not move a group between sources.

### When a source pauses

At `max_open_ranges`, `max_pending_group_obligations`, or `max_source_late_ids`, a source pauses
admission of new gaps instead of halting. It commits the longest safe prefix of a poll, keeps
polling, rechecking and closing existing gaps, advances its successful-query clock, and releases
eligible strict events. Nothing is silently lost. Readiness fails while the source reports
`capacity-exhausted`; liveness can still pass. The source logs:

```text
source paused: {setting} limit {limit} reached ({count}); open gaps continue and new gaps wait
source resumed after {secs}s paused
```

The range and pending-obligation admission charges fall when open gaps fill or finish their
deadline scans. Orphan replay markers can keep the obligation limit full; replay or reset the
orphan, or raise the limit. The late-ID count falls after groups finish closed gaps. A healthy
strict group may still be waiting behind a gap that never fills, a close-out retry, or a long
strict hold. Check source `open_range_count`, `late_id_count`, and `pending_obligation_count`, the
affected groups' status, and gap and close-out logs if a pause lasts longer than the configured
window and hold plus scan and close-out time. Failed reads and other gaps waiting their turn can
extend it.

Before a late-ID pause, the source collapses pending gaps of parked or operator-halted groups into
replay markers and frees the ranges only those groups need. The group then needs replay from its
marker, even when its other parked, history, or identity condition still appears in status; those
independent conditions need their own recovery as well. A group that can recover by itself keeps
its gaps. The late-ID limit stops *new* gaps, not recording IDs in open gaps, so a single wide gap
can exceed it. Raise a limit when observed gap rate, strict hold, close-out time, or orphan markers
make normal work repeatedly pause, after checking why the state remains retained.

### When close-out misses an event

If SG does not return an event the source saw, the group leaves the gap pending and retries with
backoff. It gives up IDs still missing on a complete read after one `late_event_window_secs` has
passed since its first confirmed miss in this process. Failed reads or many gaps retrying at once
can make that later. A strict group confirms each skipped page before delivering higher IDs and
warns once per page; an eager group warns at completion. These group log lines name the affected
IDs as runs:

```text
SG did not return {count} events the source saw in IDs {low}-{high}: {runs}; retrying, and skipping them if still missing after {secs}s
gave up on {count} events SG did not return in IDs {low}-{high}: {runs}; replay the group from {first} to run them if they become readable
no events in IDs {low}-{high} on two reads {secs}s apart; skipped them; replay the group from {low} if they appear
```

The last line comes from catch-up passing a gap with no source range after two empty reads. A gap
the source gave up can appear here again after its range was pruned. Replay the group from the
named ID if an event later becomes readable. The retry window is the daemon's allowance, not a
proven bound on SG replica lag. Delivered late IDs leave marks on disk until the group completes
their gap; retries, failed reads, and a restart can extend that retention. See
[capacity](#capacity) for sizing. Events at or below a group's delivery floor are skipped on every
path. The audited command that set the floor is their record; no per-event audit is written.

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
site worker checkpoints its source files; the supervisor checkpoints `supervisor.db`. A checkpoint
bound logs a warning and reports `checkpoint-stalled` without stopping the worker. If a source WAL
grows beyond its bound, daemon writers pause until the WAL can be reused. A WAL file can remain
larger than the bound until the next truncation after a `RESTART` run by a connection outside the
daemon. Monitor its physical size, free space, and checkpoint warnings. A corrupt source file stops
every group on that source. A write held beyond its deadline terminates its holder's worker: if it
is a site worker, the supervisor also restarts that site's named and dedicated workers; if it is a
named or dedicated worker, other workers continue.

If startup reports `source-database-uninitialized`, an interrupted source-file creation left a
schema without source metadata. Stop the daemon and remove that source file; it contains no
frontier or group state. The daemon creates it again on startup.

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

Choose [SQLite durability](#durability) for your recovery needs. On EBS, the default `"normal"`
mode avoids the per-commit synchronization cost of `"full"`. Measure `"full"` throughput on the
actual volume before relying on it for your workload; fsync latency can limit a single writer.

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

Start with the rate and size of events your callbacks actually handle:

1. Tune [count and byte limits](#queue-and-frame-limits) together.
2. Size [late-event buffers](#late-event-limits) for your window, strict hold, and close-out time.
3. Set worker memory and file limits using the [memory](#worker-memory) and
   [file-descriptor](#file-descriptors) guidance.
4. Measure peak memory, database growth, and callback throughput under your workload. The
   [measured workloads](#measured-workloads) below give context, not deployment limits.

A strict group behind after a burst catches up when arrivals fall below its delivery rate. Estimate
recovery time as one holdback plus the backlog divided by the difference between the delivery and
arrival rates. If arrivals keep up with delivery, the backlog cannot drain.

### Queue and frame limits

Tune count and byte limits together: `queue_max_events` with `queue_max_bytes`, `strict_max_events`
with `strict_max_bytes`, and `batch_size` with `max_event_bytes` and `commit_batch_max_bytes`.
Every buffer that holds complete frames must fit one maximum-size event, and validation rejects
limits that cannot. Startup also checks that each worker's health report fits
`max_ipc_frame_bytes`.

| Setting                          | Table                             | Default |
| -------------------------------- | --------------------------------- | ------- |
| `queue_max_events`               | `[sites.<site>.sources.<source>]` | 1,000   |
| `queue_max_bytes`                | `[sites.<site>.sources.<source>]` | 64 MiB  |
| `strict_max_events`              | `[sites.<site>.sources.<source>]` | 20,000  |
| `strict_max_bytes`               | `[sites.<site>.sources.<source>]` | 256 MiB |
| `batch_size`                     | `[sites.<site>.sources.<source>]` | 500     |
| `max_concurrent_catchup_readers` | `[daemon]`                        | 4       |
| `max_event_bytes`                | `[sites.<site>.sources.<source>]` | 4 MiB   |
| `commit_batch_max_bytes`         | `[sites.<site>.sources.<source>]` | 64 MiB  |
| `max_ipc_frame_bytes`            | `[daemon]`                        | 8 MiB   |

### Late-event limits

Set these limits under `[sites.<site>.sources.<source>]`:

| Setting                         | Default | Meaning                                                     |
| ------------------------------- | ------- | ----------------------------------------------------------- |
| `max_open_ranges`               | 500     | Open missing-ID range slots                                 |
| `max_range_query_pages`         | 100     | Pages in one missing-ID recheck or range-closing scan cycle |
| `max_pending_group_obligations` | 6,500   | Pending obligations across this source                      |
| `max_source_late_ids`           | 500,000 | Late IDs retained across this source                        |
| `max_late_ids_per_group`        | 50,000  | Late IDs retained for one group                             |

`late_event_window_secs` sets how long the source waits for an ID that never appears; see
[Set the late-event window](#set-the-late-event-window). `health_interval_secs` and
`supervisor_poll_interval_secs` in `[daemon]` set the health and progress intervals described under
[probe timing](#probe-timing).

For sustained bulk work with strict groups, size `max_source_late_ids` for the late-event rate
times the larger of `late_event_window_secs` and the longest strict hold, plus one window for a
close-out give-up, with headroom. A missing lower ID can retain the late IDs of every later gap
until the strict group finishes it. The 500,000 default provides room for bulk work, but retained
IDs cost about 100 bytes each on disk; each group can also keep roughly 180 bytes per delivered
late-ID mark until its gap finishes. Wide gaps can exceed admission limits, and writes and deletes
also grow the SQLite WAL between checkpoints. Size `strict_max_events` from the observed event rate
and `strict_max_bytes` from the framed-byte rate over the larger of `late_event_window_secs` and
the longest strict hold, plus another full `late_event_window_secs` if close-out confirms a missing
source-listed ID and retries before give-up. Add headroom for final-scan delay and close-out
throughput.

### Recover from a capacity limit

When a group halts on late-event capacity, raise `max_late_ids_per_group` and restart the daemon.
The group resumes once every queued late event fits; if it is still halted, raise the limit
further. `status` reports admitted late events, not queued ones.

When adding an event would exceed `strict_max_events` or `strict_max_bytes`, the source detaches
the affected strict group from its live channel, even if it is the source's only strict group. The
group catches up and keeps retrying its live join at the usual spacing. It rejoins by itself when
capacity permits; repeated capacity-caused join refusals or aborts do not halt it. If arrivals keep
outpacing delivery, the group stays in catch-up and reports its lag in health.

The source logs the first line below at INFO once per detach. The group logs the second at INFO
only when catch-up actually begins, and the third at WARNING once per halt per process unless
another warning already reports that halt. Braces stand for the affected group, reason, or
condition:

```text
detached group {group} from its live channel: {reason}
live channel detached: {reason}; rejoining through catch-up
group halted: {condition}; delivery stops until it is cleared
```

At other group limits, the daemon stops or detaches the affected group before it advances; source
gap limits [pause new admission](#when-a-source-pauses). Neither silently skips an event.

Set limits for a particular site from what you observe: event gaps, status fields such as
`outstanding_late_id_count` and `queue_byte_count`, database growth, memory use, and callback
throughput. The numeric distance between `EventLogEntry` IDs is not a memory estimate. Measure on
the storage and CPU budget that will run the daemon. The displayed late-ID fields have the
[health refresh delay](#check-status-and-health) described above.

### Worker memory

Size worker memory for event pages fetched from SG, source and group queues, strict buffers, one
in-flight event per group, IPC frames, decoded Python objects, and any copies your plugins keep.
Event contents stay in bounded memory and private IPC frames; SQLite stores only control state.

For each worker, the conservative serialized-byte sizing formula adds one provider page per owned
source poller and one per concurrent catch-up reader (`batch_size * max_event_bytes` per page),
each owned source's sender queues, strict buffer, and commit chunk, each hosted group's inbound
queue and in-flight event, and the bounded send and receive buffers of cross-process channels. At
the defaults, one source poller and four catch-up readers can hold five provider pages, each
`500 * 4 MiB = 2,000 MiB` under the maximum event size. This theoretical page term alone is 10,000
MiB; real event sizes determine what the worker actually holds. For one source and thirteen
in-process groups with ordinary small provider pages, an illustrative subtotal is about 507 MiB: 64
MiB of distinct live fanout frames (assuming the groups share one queued window), 256 MiB of strict
frames, a 64 MiB commit chunk, up to 58 MiB of SQLite page caches, and 13 times 5 MiB for an
in-flight maximum-size event and manifest. The shared 64 MiB fanout subtotal is an assumption about
this topology, not a general cap across channels. Provider pages, remote lane copies and socket
buffers, decoded Python objects, and copies retained by plugins add to this subtotal. Use the full
formula and measured peak resident memory under your workload when setting a container limit; the
subtotal is not a resident-memory ceiling.

### File descriptors

A site worker with one source, G in-process groups, and P plugins per group uses about
`19 + G * (6 + P)` open file descriptors. Each remote lane adds two to the site worker; the
supervisor uses about five per existing group during startup reconciliation. The supervisor raises
its soft file limit before reconciliation, up to its hard limit or 65,536 (with a 10,240 retry if
the kernel refuses), and workers inherit it. Startup refuses a configuration whose workers or
supervisor still exceed the limit, allowing 32 descriptors for temporary opens; the error names the
worker, need, and limit. Set the host's hard limit high enough for the planned group count.

### Measured workloads

Use these measurements to understand the effect of placement and group count. Real callbacks add
their own work; measure your workload on its storage and CPU budget before choosing a capacity
target. Burst rates and sustained storage rates describe different workloads.

#### Dedicated-worker bursts

Environment: Apple M1 Max, 10 logical CPUs, 64 GiB RAM, macOS 27.2, APFS on SSD. A source and each
group run in dedicated workers. The callback writes one record per event, with 4,000 warm-up events
followed by a 10,000-event burst. Sampled host load is at most 58.9% busy.

Rates count matched callbacks over the same interval for all groups: from 250 ms after the final
burst event is published to every live channel until the first group has 1,000 callbacks left. Each
interval lasts at least three seconds, with published backlog at both ends and no capacity detach.
The median eager-alone rate is 1,814.50 callbacks/s across three observations. Across five paired
observations, the median of each pair's lower rate is 1,245.66 callbacks/s per group.

| 10,000-event burst      | Common interval |   Matched callbacks/s per group | Total matched callbacks/s | Acquisition and hold ms per callback  |
| ----------------------- | --------------: | ------------------------------: | ------------------------: | ------------------------------------- |
| Eager alone, run 1      |         3.983 s |                  1,793.78 eager |                  1,793.78 | 0.033/0.203 eager                     |
| Eager and strict, run 1 |         3.364 s | 1,245.66 eager; 1,249.23 strict |                  2,494.88 | 0.182/0.244 eager; 0.152/0.257 strict |
| Eager and strict, run 2 |         3.870 s | 1,122.07 eager; 1,086.15 strict |                  2,208.23 | 0.200/0.272 eager; 0.179/0.286 strict |
| Eager alone, run 2      |         3.920 s |                  1,814.50 eager |                  1,814.50 | 0.032/0.202 eager                     |
| Eager and strict, run 3 |         3.250 s | 1,283.07 eager; 1,292.30 strict |                  2,575.37 | 0.176/0.236 eager; 0.143/0.250 strict |
| Eager and strict, run 4 |         3.365 s | 1,250.51 eager; 1,239.21 strict |                  2,489.72 | 0.180/0.244 eager; 0.149/0.258 strict |
| Eager alone, run 3      |         3.975 s |                  1,821.88 eager |                  1,821.88 | 0.031/0.202 eager                     |
| Eager and strict, run 5 |         3.180 s | 1,287.23 eager; 1,279.68 strict |                  2,566.91 | 0.175/0.238 eager; 0.149/0.251 strict |

Acquisition time includes local and file-lock waiting, SQLite transaction acquisition, and
scheduling. Hold time runs through commit. These are sampled burst rates for this host and
callback, not sustained capacity, a latency bound, or a guarantee for another workload.

#### Sustained storage work

These rates count matched callbacks summed across groups sharing one source database. Callbacks do
no external work. The Linux environment is an `m7i-flex.large` with ext4 state on EBS gp3; the
macOS environment uses APFS.

| Environment          | One group, callbacks/s | Two groups, total callbacks/s | Thirteen groups, total callbacks/s | Thirteen groups, source events/s |
| -------------------- | ---------------------: | ----------------------------: | ---------------------------------: | -------------------------------: |
| macOS, APFS          |               1,685.64 |                      1,395.40 |                           1,303.72 |                           100.26 |
| Linux, EBS gp3, ext4 |               2,393.60 |                      1,991.56 |                           1,842.32 |                          141.717 |

The APFS figures are medians. These storage-only rates do not imply that the broader workload meets
its latency or storage limits. Groups sharing one source take turns writing its database, so adding
groups reduces the capacity available to each. Use the
[storage contention gate](development.md#storage-contention-gate) to measure your storage workload.

`normal` is qualified for the tested workload on `m7i-flex.large` with baseline EBS gp3 (100 GiB
state and 30 GiB root). The dedicated catch-up cell completed three full-length runs; its
all-commit maxima were 50.770, 36.690 and 34.794 ms against the unchanged 500 ms target. All NORMAL
work drained, and every offered identity was accounted for.

`full` capacity remains unqualified on this host. Its retained capacity observations include:

| FULL workload                   |                                                            Observed value |            Capacity target |
| ------------------------------- | ------------------------------------------------------------------------: | -------------------------: |
| Worst-case, single process      |                                                      Commit p99 58.123 ms |                      50 ms |
| Worst-case, dedicated processes |   Commit p99 903.051 ms; p999 997.645 ms; all-commit maximum 1,061.902 ms |    50 ms; 250 ms; 1,000 ms |
| Lifecycle, worst of three runs  | All-commit maximum 43,336.031 ms; 7,356 outstanding arrivals at drain end | 1,000 ms; zero outstanding |

The FULL lifecycle debt is fully accounted for: arrivals were unattempted, pending attempts later
expired, and one pending arrival completed after drain in each run. FULL at this workload needs a
larger machine; additional vCPUs alone have not been shown to resolve serialized writes. The NORMAL
qualification does not establish FULL capacity.

## Configuration reference

Names for sites, credentials, sources, groups, workers, plugins, and callbacks use 1-64 lowercase
letters, digits, `-`, or `_`, starting with a letter. Paths in the configuration are resolved
relative to the configuration file. Run `validate-config` after changing a key.

### `[daemon]`

| Key                              | Default   | Meaning                                                                                                |
| -------------------------------- | --------- | ------------------------------------------------------------------------------------------------------ |
| `sg_api_provider`                | Required  | `shotgun-api3` or `sg-python-api`. Async groups using the latter need the `sg-python-api-async` extra. |
| `state_dir`                      | Required  | Directory for durable state and local sockets.                                                         |
| `probe_bind`                     | `0.0.0.0` | IP address for HTTP probes; see [HTTP probes](#http-probes).                                           |
| `probe_port`                     | Disabled  | TCP port that enables HTTP probes.                                                                     |
| `sqlite_synchronous`             | `normal`  | SQLite durability mode: `normal` or `full`.                                                            |
| `checkpoint_interval_secs`       | 1         | Interval between worker WAL checkpoint attempts.                                                       |
| `health_interval_secs`           | 2         | Interval between worker health updates.                                                                |
| `worker_startup_deadline_secs`   | 30        | Time allowed for a worker to complete startup.                                                         |
| `supervisor_poll_interval_secs`  | 1         | Supervisor progress and worker poll interval.                                                          |
| `shutdown_deadline_secs`         | 10        | Time allowed for each worker's graceful shutdown.                                                      |
| `async_cancel_grace_secs`        | 5         | Grace period for cancelling an async callback.                                                         |
| `max_ipc_frame_bytes`            | 8,388,608 | Largest accepted IPC frame body.                                                                       |
| `max_plugin_config_bytes`        | 262,144   | Largest canonical plugin configuration, in bytes.                                                      |
| `max_manifest_bytes`             | 1,048,576 | Largest callback manifest, in bytes.                                                                   |
| `max_failure_text_bytes`         | 262,144   | Largest persisted failure detail, in bytes.                                                            |
| `max_group_obligations`          | 1,000     | Per-group obligation bound; must be at least each source's `max_open_ranges`.                          |
| `max_pending_epochs`             | 8         | Bound on pending strict epochs; minimum 2.                                                             |
| `max_concurrent_catchup_readers` | 4         | Concurrent catch-up reader limit.                                                                      |

### `[sites.<site>]` and credentials

| Key              | Default  | Meaning                                   |
| ---------------- | -------- | ----------------------------------------- |
| `server`         | Required | SG site URL.                              |
| `default_source` | Required | Source used by groups that omit `source`. |
| `credentials`    | Required | Named credential tables.                  |
| `sources`        | Required | Named source tables.                      |
| `groups`         | Required | Named group tables.                       |
| `workers`        | Empty    | Named worker tables.                      |

Each `[sites.<site>.credentials.<name>]` accepts:

| Key                         | Default  | Meaning                                               |
| --------------------------- | -------- | ----------------------------------------------------- |
| `script_name`               | Required | SG script name.                                       |
| `api_key`                   | Required | Secret reference, such as `{ env = "SG_EVENT_KEY" }`. |
| `timeout_secs`              | 30       | Timeout for one SG request.                           |
| `max_rpc_attempts`          | 3        | Maximum attempts for one SG request.                  |
| `rpc_attempt_interval_secs` | 3        | Wait between SG request attempts.                     |

### `[sites.<site>.sources.<name>]`

| Key                             | Default     | Meaning                                                                 |
| ------------------------------- | ----------- | ----------------------------------------------------------------------- |
| `credentials`                   | Required    | Named credential used to poll this source.                              |
| `fetch_interval_secs`           | 1           | Poll interval when the source is healthy.                               |
| `batch_size`                    | 500         | Maximum records requested in one poll page.                             |
| `late_event_window_secs`        | 300         | Time to wait for a missing ID before its final scan.                    |
| `poll_retry_base_secs`          | 1           | Initial delay after a failed SG poll.                                   |
| `poll_retry_max_secs`           | 60          | Maximum failed-poll delay; at least the base delay.                     |
| `poll_retry_multiplier`         | 2           | Delay growth factor; at least 1.                                        |
| `poll_retry_jitter_fraction`    | 0.2         | Random delay fraction from 0 inclusive to 1 exclusive.                  |
| `max_event_bytes`               | 4,194,304   | Largest encoded event.                                                  |
| `commit_batch_max_bytes`        | 67,108,864  | Maximum complete-frame bytes in one commit batch.                       |
| `queue_max_events`              | 1,000       | Maximum events in an eager delivery queue.                              |
| `queue_max_bytes`               | 67,108,864  | Maximum bytes in an eager delivery queue.                               |
| `strict_max_events`             | 20,000      | Maximum held events for strict delivery.                                |
| `strict_max_bytes`              | 268,435,456 | Maximum held bytes for strict delivery.                                 |
| `max_open_ranges`               | 500         | Maximum concurrently open missing-ID ranges.                            |
| `max_range_query_pages`         | 100         | Maximum SG pages in one missing-ID recheck or range-closing scan cycle. |
| `max_pending_group_obligations` | 6,500       | Source-wide pending group obligation bound.                             |
| `max_source_late_ids`           | 500,000     | Source-wide retained late-ID bound.                                     |
| `max_late_ids_per_group`        | 50,000      | Retained late-ID bound for one group.                                   |

The [capacity guide](#capacity) explains how to size the byte and late-event limits together.

### Groups, plugins, and workers

Each `[sites.<site>.groups.<name>]` accepts:

| Key                      | Default                 | Meaning                               |
| ------------------------ | ----------------------- | ------------------------------------- |
| `plugins`                | Required                | Ordered array of plugin tables.       |
| `source`                 | Site's `default_source` | Source that feeds this group.         |
| `callback_credentials`   | Source credential       | Credential available to callbacks.    |
| `execution`              | `sync`                  | `sync` or `async` callback execution. |
| `ordering`               | `eager`                 | `eager` or `strict` delivery.         |
| `strict_holdback_secs`   | 2, for strict groups    | Minimum strict hold before delivery.  |
| `worker`                 | `default`               | Default, dedicated, or named worker.  |
| `callback_deadline_secs` | 120                     | Maximum callback time, in seconds.    |

Each `[[sites.<site>.groups.<name>.plugins]]` accepts:

| Key           | Default                   | Meaning                                     |
| ------------- | ------------------------- | ------------------------------------------- |
| `id`          | Required                  | Plugin identity.                            |
| `path`        | Required                  | Python plugin file.                         |
| `credentials` | Group callback credential | Credential available to this plugin.        |
| `config`      | Empty                     | Non-secret JSON-compatible plugin settings. |
| `secrets`     | Empty                     | Named environment-backed secret references. |

Each `[sites.<site>.workers.<name>]` accepts:

| Key                   | Default | Meaning                                                          |
| --------------------- | ------- | ---------------------------------------------------------------- |
| `restart_limit`       | 5       | Failures allowed in the restart window before the breaker opens. |
| `restart_window_secs` | 300     | Window for counting worker failures.                             |
| `healthy_reset_secs`  | 300     | Healthy runtime needed to clear failure backoff.                 |

## Exit codes

`sg-event-daemon --help` and `sg-event-daemon --version` print to standard output and exit with
status 0. Every `sg-event-daemon` command uses these exit codes:

| Code | Meaning                                                                                                                                                                                                                                                                        |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 0    | Success. For `status --check`, the check passed. A supervisor stopped with Ctrl-C or SIGTERM exits 0 after a clean shutdown.                                                                                                                                                   |
| 1    | The health check failed, a recovery command was refused or failed, or a runtime error occurred, including a failed or invalid SG request during `validate-config --live`. A refused or failed recovery may already have changed state in an earlier step, as its message says. |
| 2    | The command line, configuration, acceptance evidence, provider, or plugin checked by `validate-config` is invalid, or a configuration or evidence file cannot be read.                                                                                                         |
| 3    | The outcome is unknown: the control socket is unavailable, its reply could not be understood, or the command did not finish in time and may still complete.                                                                                                                    |
| 130  | Ctrl-C interrupted the command before it finished.                                                                                                                                                                                                                             |

A failed command prints one line on standard error that starts with `error:`. A usage error prints
the command's usage line first, and a failed `status --check` prints only its JSON result. If a
reader closes standard output early, for example when you pipe `status` output into `head`, the
command stops writing and keeps the exit code of the work it completed.

Under `supervise`, a plugin import or registration failure is a worker failure. The supervisor
restarts that worker with backoff until its breaker opens; it does not exit with code 2 for that
plugin failure.

Recovery commands can report `source-busy` when a call expires while queued at its source before
the source starts it. Once started, the call waits for its actual result. A command that needs to
store a supervisor transition can report `supervisor-database-busy` when another connection holds
`supervisor.db` beyond its write attempt. End that connection and retry after checking status.
