# sg-event-daemon

sg-event-daemon runs your Python plugins in response to Autodesk Flow Production Tracking events.
It watches `EventLogEntry` records, delivers each event to the plugins that want it, and records
delivery progress in SQLite. After a crash or restart it resumes where it stopped, including an
event whose callbacks had only partly finished.

It is for pipeline teams whose event-driven integrations need controlled recovery and health
checks. One foreground supervisor can serve several sites and plugin groups. Groups can share one
event poller while keeping separate ordering, failure handling, and recovery, so a failing plugin
stops only its own group.

This documentation uses SG for Flow Production Tracking, formerly named Shotgun and ShotGrid.

## Why use it?

The first-party [shotgunEvents](https://github.com/shotgunsoftware/shotgunevents) daemon remains a
good fit for existing plugins and simple deployments. It loads its plugins directly, reloads
changed plugin files, daemonizes itself, and can send logs to files and email. sg-event-daemon
makes different tradeoffs:

| Need                  | sg-event-daemon behavior                                                                                                              |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Recover after a crash | Records its position, the event in progress, and each completed callback in SQLite. Event contents are refetched from SG, not stored. |
| Isolate workloads     | Polls each event source once for any number of plugin groups. Each group delivers serially and fails independently.                   |
| Control ordering      | Offers low-latency `eager` delivery, or `strict` delivery in increasing event ID order at the cost of a holdback delay.               |
| Operate failures      | Provides status and health probes, callback deadlines, worker restart limits, and audited recovery commands.                          |
| Write modern plugins  | Runs synchronous callbacks, and native asynchronous callbacks with the `sg-python-api-async` extra.                                   |

Weigh these limits before you adopt it:

- shotgunEvents plugins do not run unmodified. Port each one to explicit registration, as the
  [porting guide](docs/porting.md) shows.
- A new plugin group starts at the latest visible event. The daemon does not import the event
  position saved by shotgunEvents.
- Plugin files are not reloaded while the daemon runs. Restart the daemon to load changed plugin
  code.
- The daemon runs in the foreground. A service manager such as systemd or Kubernetes owns process
  restarts, log capture, and alerts.
- Delivery is at least once. A callback can run again after a crash, retry, or replay, so its side
  effects must tolerate repetition.

## Installation

You need:

- Linux or macOS.
- Python 3.11 or newer whose SQLite library is version 3.51.3 or newer. The daemon checks the
  SQLite version before it opens state.
- Exactly one SG API provider. `shotgun-api3` supports synchronous groups; `sg-python-api` supports
  synchronous and asynchronous groups.
- An SG site and script credentials that can read events and perform your plugins' work.
- A private, persistent state directory on local storage, owned by the user that runs the daemon.

From a checkout, install the command with [uv](https://docs.astral.sh/uv/) and the standard
Autodesk `shotgun-api3` provider:

```sh
uv tool install '.[shotgun-api3]'
```

To use the `sg-python-api` provider instead, install the `sg-python-api` extra for synchronous
groups or the `sg-python-api-async` extra for asynchronous groups. Install only one provider: both
supply the `shotgun_api3` module, and the daemon refuses to run when both are present.

Python's version alone does not determine its SQLite version. Check the SQLite library in the
installed tool's environment:

```sh
"$(uv tool dir)/sg-event-daemon/bin/python" -c 'import sqlite3; print(sqlite3.sqlite_version)'
```

## Quick start

This example sets up one plugin that logs each new event, then starts the daemon in the foreground.

### Write a plugin

Save this plugin as `/etc/sg-event-daemon/plugins/observe.py`:

```python
def on_event(context):
    context.logger.info('EventLogEntry %s', context.event['id'])


def register(registrar):
    registrar.add_callback(id='log-event', callback=on_event)
```

### Configure the daemon

Save this configuration as `/etc/sg-event-daemon/daemon.toml`, replacing the site URL and script
name with your own:

```toml
[daemon]
state_dir = "/var/lib/sg-event-daemon"
sg_api_provider = "shotgun-api3"

[sites.studio]
server = "https://studio.example.invalid"
default_source = "primary"

[sites.studio.credentials.event-reader]
script_name = "event-daemon-reader"
api_key = { env = "SG_EVENT_READER_KEY" }

[sites.studio.sources.primary]
credentials = "event-reader"

[sites.studio.groups.observers]
source = "primary"
callback_credentials = "event-reader"
execution = "sync"

[[sites.studio.groups.observers.plugins]]
id = "event-log"
path = "plugins/observe.py"
```

The script key comes from the `SG_EVENT_READER_KEY` environment variable, so the configuration file
holds no secrets. Relative plugin paths and `state_dir` resolve from the directory of the
configuration file after symlinks are resolved. If `state_dir` is missing, the daemon creates it,
which requires write access to its parent. The directory must be owned by the daemon's user with
mode `0700`.

### Validate and start

With `SG_EVENT_READER_KEY` set in the environment, validate the configuration and plugin, then
start the supervisor:

```sh
sg-event-daemon validate-config --config /etc/sg-event-daemon/daemon.toml
sg-event-daemon supervise --config /etc/sg-event-daemon/daemon.toml
```

Validation imports the plugin without contacting SG. It warns that importing plugin code can have
side effects, then prints `{"provider":"shotgun-api3","sites":["studio"],"validated":true}`.
Invalid configuration, including a missing plugin file, makes either command print `error:` and
exit with status 2. Invalid plugin registration makes `validate-config` exit with status 2. During
`supervise`, a plugin import or registration failure stops its worker; the supervisor restarts that
worker until its breaker opens.

The supervisor runs until you stop it with Ctrl-C or SIGTERM, which shuts down its workers and
exits with status 0. The new group starts at the latest visible event rather than processing
existing history. When someone changes something in SG, the supervisor's output shows an
`EventLogEntry` line from the plugin.

### Check status

In another shell, ask the running supervisor for its status:

```sh
sg-event-daemon status --config /etc/sg-event-daemon/daemon.toml
```

The command prints one line of JSON. `primary_state` summarizes overall health, for example `idle`
while the group waits for events, and `liveness` and `readiness` report the health checks. If no
supervisor is running for that state directory, the command prints one line that starts with
`error:` and exits with status 3.

This example only logs events. A plugin that writes to SG should use separate callback credentials
with the permissions it needs; see
[separate reader and writer credentials](docs/operations.md#separate-reader-and-writer-credentials).

## Documentation

| If you want to...                              | Read                                                                     |
| ---------------------------------------------- | ------------------------------------------------------------------------ |
| Port a shotgunEvents plugin or write a new one | [Porting plugins](docs/porting.md)                                       |
| Test a plugin without a daemon or SG site      | [Offline harness](docs/porting.md#test-plugins-offline)                  |
| Give plugins separate credentials for writing  | [Credentials](docs/operations.md#separate-reader-and-writer-credentials) |
| Choose between eager and strict ordering       | [Ordering](docs/operations.md#choose-eager-or-strict-ordering)           |
| Monitor health or configure probes             | [Status and probes](docs/operations.md#check-status-and-health)          |
| Recover a failed or stopped group              | [Recovery](docs/operations.md#recover-a-group-or-source)                 |
| Back up state or plan storage                  | [State and backups](docs/operations.md#sqlite-state-and-backups)         |
| Run on Kubernetes                              | [Kubernetes](docs/operations.md#kubernetes)                              |
| Size queues, buffers, and memory               | [Capacity](docs/operations.md#capacity)                                  |
| See changes by version                         | [Changelog](CHANGELOG.md)                                                |

## Development

See the [development guide](docs/development.md) for the required tools, CI, and dependency locks.
With those tools installed, run:

```sh
make setup
make check
```

The checks never contact a live SG site; the development guide describes the opt-in live tests.

## License

sg-event-daemon is available under the [MIT License](LICENSE). It is an independent project and is
not affiliated with or endorsed by Autodesk. Flow Production Tracking is a product of Autodesk,
Inc.
