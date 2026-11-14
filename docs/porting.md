# Porting plugins

This guide is for plugin authors who move shotgunEvents plugins to sg-event-daemon or write new
plugins for it. It explains registration, credentials, the callback context, event filters, and
offline testing. To run a first plugin, start with the
[README quick start](../README.md#quick-start); the [operations guide](operations.md) covers
deployment and recovery.

| If you want to...                   | Start here                                                                                                         |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Port a shotgunEvents plugin         | [What changes](#what-changes), then [Before and after](#before-and-after).                                         |
| Move secrets out of code            | [Credentials and secrets](#credentials-and-secrets).                                                               |
| Initialize a plugin                 | [Registration and setup](#registration-and-setup).                                                                 |
| Test without a daemon or SG site    | [Test plugins offline](#test-plugins-offline).                                                                     |
| Look up context fields or filters   | [Callback context](#callback-context) and [Event filters](#event-filters).                                         |
| Choose grouping or execution mode   | [Groups](#groups-ordering-and-failures), [Sync and async](#sync-and-async), and [Shared workers](#shared-workers). |
| Inspect harness settings or results | [Harness reference](#harness-reference).                                                                           |

## What changes

sg-event-daemon keeps the shotgunEvents model of Python callbacks that receive `EventLogEntry`
events, but it does not run shotgunEvents plugins unmodified. A port moves credentials into the
daemon configuration, replaces registration with explicit ids, and places the plugin in a group
(`[sites.<site>.groups.<name>]`), an ordered set of plugins that shares one ordering and failure
domain.

| shotgunEvents                                                                | sg-event-daemon                                                                                   |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `registerCallbacks(reg)`, found by scanning plugin directories               | `register(registrar)`, in a file listed in the daemon configuration                               |
| `reg.registerCallback(script_name, script_key, callback, matchEvents, args)` | `registrar.add_callback(id=..., callback=..., events=...)`, with credentials and settings in TOML |
| `callback(sg, logger, event, args)`                                          | `callback(context)`, with `context.sg`, `context.logger`, `context.event`, and `context.config`   |
| A failing callback is deactivated by default, and other callbacks continue   | A failing callback stops its group until an operator decides what to do                           |
| Changed plugin files are reloaded                                            | Restart the daemon to load changed plugin code                                                    |

## Before and after

A typical shotgunEvents plugin combines credentials, registration, callback arguments, and callback
code:

```python
SCRIPT_NAME = 'event-daemon'
SCRIPT_KEY = 'plaintext-key'


def on_shot_change(sg, logger, event, args):
    if args['project_id'] == event['project']['id']:
        sg.update('Shot', event['entity']['id'], {'sg_seen_by_daemon': True})


def registerCallbacks(reg):
    reg.registerCallback(
        SCRIPT_NAME,
        SCRIPT_KEY,
        on_shot_change,
        {'Shotgun_Shot_Change': ['sg_status_list']},
        {'project_id': 123},
    )
```

The ported module has a synchronous `register(registrar)` function and an explicit callback id:

```python
def register(registrar):
    project_id = registrar.config['project_id']

    def on_shot_change(context):
        project = context.event['project']
        if project is not None and project['id'] == project_id:
            context.sg.update(
                'Shot',
                context.event['entity']['id'],
                {'sg_seen_by_daemon': True},
            )

    registrar.add_callback(
        id='mark-shot-seen',
        callback=on_shot_change,
        events={'Shotgun_Shot_Change': ('sg_status_list',)},
    )
```

Its plugin id, credentials, and settings move to the daemon configuration:

```toml
[[sites.studio.groups.editorial.plugins]]
id = "shot-status"
path = "/opt/sg-plugins/shot_status.py"
credentials = "callback-writer"
config = { project_id = 123 }
```

The closure replaces the shotgunEvents `args` argument. `registrar.config` is a deeply frozen copy
of the plugin's `config` table, which must be JSON-compatible and must not contain secrets.

Keep plugin and callback IDs stable. If registration changes while an event is in progress, the
group can stop with `shape-mismatch`; see [registration identity](#registration-identity).

## Credentials and secrets

Source credentials, which poll for events, decide which `EventLogEntry` records are visible.
Callback credentials belong to a group or plugin and can have narrower or different permissions.
Move both out of plugin code:

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

[[sites.studio.groups.editorial.plugins]]
id = "shot-status"
path = "/opt/sg-plugins/shot_status.py"
secrets.webhook_token = { env = "SHOT_STATUS_WEBHOOK_TOKEN" }
```

Environment variables named by `api_key` or a plugin's `secrets` table must be set wherever the
full configuration loads: for `validate-config` and `supervise`, whose workers inherit its
environment. Status and recovery commands do not need them.

Workers read secret values when plugin setup starts. The context exposes each one as an immutable
`SecretValue` whose string and representation are always `<redacted>`, so reading the value is a
deliberate call:

```python
def send_notification(context):
    token = context.secrets['webhook_token'].reveal()
    send_to_service(token, context.event)
```

Anything a plugin prints, logs, includes in an exception, or transmits after `reveal()` is outside
the daemon's redaction guarantee. Do not place secrets in `config`, in failure text derived from
events, or in module globals.

## Registration and setup

Importing a configured plugin runs its top-level Python code, both in workers and during
`validate-config`. The daemon is not a sandbox: trust each plugin file and every module it imports.

`register(registrar)` must be synchronous and should only declare an optional setup function and
callbacks. It also runs during validation, before any SG connection exists, so it must not make SG
requests, start threads or tasks, scan directories, or depend on filename discovery. The
registration is fixed when `register` returns, and later calls to the registrar raise an error.

The daemon imports each configured plugin as a separate module. A plugin's setup and callbacks
share its module state, but the same file configured twice, in two groups or under two plugin ids,
gets two independent copies.

Use an optional setup function for initialization that needs the plugin's SG handle:

```python
def setup(context):
    context.logger.info('starting plugin %s', context.plugin_id)


def callback(context):
    context.logger.info('received event %s', context.event['id'])


def register(registrar):
    registrar.set_setup(setup)
    registrar.add_callback(id='observe', callback=callback)
```

Setup runs when a worker starts the group, before its first callback, and again when an operator
retries a failed setup, so make it idempotent. A setup failure stops the group in the same way as a
callback failure.

### Registration identity

Keep plugin and callback IDs stable. For an event in progress, the daemon records which callbacks
still need to run, identified by plugin id, callback id, order, event filter, configuration, secret
references, and credential. If any of these change while an event is in progress, for example after
a callback fails, the group stops with `shape-mismatch` until you restore the previous registration
or an operator runs [`restart-event`](operations.md#retry-skip-or-restart-the-current-event).
Changing a callback's code alone does not change its registration.

## Test plugins offline

`sg_event_daemon.testing` loads one real plugin file and drives synthetic events through the same
registration, filters, contexts, and invocation code the daemon uses. It needs no daemon, database,
or SG connection, so you can use it in a plugin's unit tests.

This example tests the ported plugin from [Before and after](#before-and-after), saved as
`shot_status.py`. `RecordingHandle` stands in for the SG handle and records the update:

```python
from pathlib import Path

from sg_event_daemon.testing import HarnessSettings, drive_sync_plugin, load_plugin


class RecordingHandle:
    def __init__(self):
        self.updates = []
        self.closed = False

    def set_session_uuid(self, session_uuid):
        pass

    def update(self, entity_type, entity_id, data):
        self.updates.append((entity_type, entity_id, data))

    def close(self):
        self.closed = True


synthetic_event = {
    'id': 42,
    'type': 'EventLogEntry',
    'event_type': 'Shotgun_Shot_Change',
    'attribute_name': 'sg_status_list',
    'meta': {},
    'entity': {'id': 123, 'type': 'Shot'},
    'user': None,
    'project': {'id': 1, 'type': 'Project'},
    'session_uuid': None,
    'created_at': '2000-01-01T00:00:00+00:00',
}

handle = RecordingHandle()
plugin = load_plugin(
    Path('shot_status.py'),
    plugin_id='shot-status',
    settings=HarnessSettings(config={'project_id': 1}),
)
result = drive_sync_plugin(plugin, (synthetic_event,), sg=handle)
assert result.ok
assert handle.updates == [('Shot', 123, {'sg_seen_by_daemon': True})]
assert handle.closed
```

The assertions check that setup and callbacks succeeded, the expected SG update was made, and the
harness closed the handle. `drive_sync_plugin` stops at the first failure and returns the result;
see [harness results](#harness-results) to inspect failures.

For an async plugin, call `load_plugin(..., execution='async')` and await `drive_async_plugin` with
the same arguments. Supply an async `close` method on its test handle; `set_session_uuid` stays
synchronous. See [harness rules](#harness-rules) for input and cleanup requirements.

## Callback context

Setup and callbacks receive these fields:

| Field or method                        | Meaning                                                                               |
| -------------------------------------- | ------------------------------------------------------------------------------------- |
| `sg`                                   | Plugin SG handle, using its callback credential. The daemon owns it; do not close it. |
| `logger`                               | Plugin logger, writing to the daemon's standard output.                               |
| `config`                               | Immutable plugin configuration.                                                       |
| `secrets`                              | Redacted secret values; use `reveal()` deliberately.                                  |
| `site`, `source`, `group`, `plugin_id` | Identity of where the plugin runs.                                                    |
| `shutdown_requested`                   | Whether the daemon has asked the worker to stop.                                      |
| `heartbeat()`                          | Extends the current deadline during legitimate long work.                             |
| `heartbeat_callback`                   | Host-supplied callable used by `heartbeat()`.                                         |
| `shutdown_check`                       | Host-supplied callable used by `shutdown_requested`.                                  |

Callbacks also receive:

| Field                | Meaning                                                                            |
| -------------------- | ---------------------------------------------------------------------------------- |
| `event`              | Immutable event.                                                                   |
| `callback_id`        | ID of the running callback.                                                        |
| `delivery_attempt`   | Attempt number, starting at 1 and recorded durably across restarts.                |
| `is_operator_replay` | True during operator replay catch-up; false after the group rejoins live delivery. |

Before each callback, the daemon applies the event's `session_uuid` to `context.sg`, so SG can show
the callback's changes in the browser session that produced the event. shotgunEvents does this only
when its `use_session_uuid` option is on.

### Deadlines and background work

Each setup or callback call must finish within the group's `callback_deadline_secs` (120 seconds by
default). Call `context.heartbeat()` during legitimate long work to extend the deadline. When one
callback overruns its deadline, the supervisor parks that group and replaces the worker without
counting the restart toward its breaker. The event stays in progress; the interrupted callback runs
again after an operator unparks the group. Other overruns take the ordinary
[worker restart path](operations.md#choose-workers).

A callback must also finish or join every thread or task it starts before it returns. The daemon
records the callback as complete when it returns and cannot track work that escapes. Background
work that still uses the event can lose ordering, bypass retry, and outlive shutdown.

A callback that calls `sys.exit()` fails like any other exception: its group stops with
`callback-failed`. Fix the callback, then
[retry, skip, or restart the current event](operations.md#retry-skip-or-restart-the-current-event).

### Callbacks must tolerate repeats

Delivery is at least once. A callback can run again when the process dies after a side effect but
before the daemon records completion, when an operator replays events from an earlier ID, or after
an operator `retry`, which increases `delivery_attempt`. Make each callback idempotent, or use a
remote idempotency key when a side effect cannot safely repeat.

## Event filters

`events` maps event types to the attribute names a callback wants:

```python
registrar.add_callback(
    id='shot-status-only',
    callback=callback,
    events={
        'Shotgun_Shot_Change': ('sg_status_list',),
        'Shotgun_Shot_New': None,
    },
)
```

| Filter                                           | Matches                              |
| ------------------------------------------------ | ------------------------------------ |
| Omitted `events`, or an empty mapping            | Every event.                         |
| An event type mapped to `None`                   | Every attribute of that event type.  |
| An event type mapped to a list or tuple of names | Those attributes of that event type. |
| `'*'` as an event-type key                       | Every event type.                    |
| `'*'` in the attribute names                     | Every attribute.                     |
| An empty attribute list                          | Nothing for that event type.         |

## Groups, ordering, and failures

Within a group, callbacks run one event at a time, in configured plugin order and then declaration
order. All plugins in a group form one serial ordering and failure domain. A failed callback stops
later callbacks and later events for the whole group, while other groups continue. Place callbacks
that depend on each other's order in one group, and split independent work into separate groups so
that one failure does not stop unrelated plugins.

SG can make an event visible after events with higher IDs. shotgunEvents waits up to 5 minutes for
such an event, runs it when it appears, and then runs again the events after it that it had already
handled. The daemon runs a late event once. In an eager group, the default, it runs soon after the
source finds it, after any higher IDs already delivered. The source checks open gaps on every poll;
its [late-event window](operations.md#set-the-late-event-window) limits the wait for IDs that never
appear. A callback that applies a late event's value can overwrite a newer one. Read the entity's
current state from SG when that matters, or use a
[strict group](operations.md#choose-eager-or-strict-ordering).

Recovery also applies to the whole group. `replay --from-event-id ID` makes `ID` the first event
delivered again to every plugin in the group, and it returns only the events SG exposes at replay
time. It is at-least-once recovery, not an exact reconstruction of history. See
[recovery](operations.md#recover-a-group-or-source) for the operator commands.

## Sync and async

A group has one execution mode, set by `execution`. Registration is synchronous in both modes.

| Mode    | Setup and callbacks     | Installation extra                | `sg_api_provider`             |
| ------- | ----------------------- | --------------------------------- | ----------------------------- |
| `sync`  | Ordinary functions.     | `shotgun-api3` or `sg-python-api` | The selected provider's name. |
| `async` | Native async functions. | `sg-python-api-async`             | `"sg-python-api"`             |

For an async group:

```python
async def setup(context):
    await context.sg.find('Project', [], ['id'], limit=1)


async def callback(context):
    await context.sg.find('Shot', [['id', 'is', context.event['entity']['id']]], ['id'])


def register(registrar):
    registrar.set_setup(setup)
    registrar.add_callback(id='async-shot', callback=callback)
```

Install one provider only. The daemon owns one persistent event loop per async group, so do not
call `asyncio.run()` inside an async callback.

## Shared workers

Plugins whose groups share a worker share one process. They must not:

- install or replace process signal handlers;
- reconfigure root logging or add shared rotating handlers;
- monkey-patch process-wide modules;
- mutate provider globals; or
- assume they own the process or event loop.

Use the context logger, and let the service manager own standard output and standard error. Give a
plugin that cannot follow these rules its own process by setting `worker = "dedicated"` on its
group. To share a process among chosen groups instead, define a named worker at
`[sites.<site>.workers.<name>]` and set `worker = "<name>"` on each of those groups. Sharing a
process changes fault isolation and resource use, not delivery semantics.

## Harness reference

Import the harness API from `sg_event_daemon.testing`. It runs one plugin with synthetic events,
without a daemon, database, or SG connection.

### Harness settings and loaded plugins

Pass a `HarnessSettings` instance to `load_plugin`:

| Setting                   | Purpose                                                  |
| ------------------------- | -------------------------------------------------------- |
| `site`, `source`, `group` | Context identity.                                        |
| `config`                  | Non-secret, JSON-compatible settings.                    |
| `secrets`                 | Mapping from secret names to environment variable names. |

`load_plugin` returns a `LoadedPlugin`:

| Field                                  | Meaning                                        |
| -------------------------------------- | ---------------------------------------------- |
| `plugin_id`, `site`, `source`, `group` | Plugin and context identity.                   |
| `execution`                            | `sync` or `async`.                             |
| `callbacks`                            | Registered callback IDs in declaration order.  |
| `config`                               | Frozen plugin configuration.                   |
| `module`                               | Imported plugin module.                        |
| `site_config`, `group_config`          | Validated configuration used by the host.      |
| `plugin_spec`                          | Registered plugin configuration and callbacks. |

`build_host(provider)` creates an unstarted host using the supplied provider. The caller owns its
lifecycle; the `drive_sync_plugin` and `drive_async_plugin` helpers manage that lifecycle for the
common testing workflow.

### Harness results

`drive_sync_plugin` and `drive_async_plugin` run setup, then matching callbacks. Both accept these
optional arguments:

| Argument             | Meaning                                                                      |
| -------------------- | ---------------------------------------------------------------------------- |
| `sg`                 | Test handle for plugin SG requests; the default rejects requests.            |
| `delivery_attempt`   | One-based attempt number, default 1; non-positive values raise `ValueError`. |
| `is_operator_replay` | Whether the synthetic delivery is an operator replay; defaults to `False`.   |

They return a `DriveResult`:

| Field or property | Meaning                                                                                           |
| ----------------- | ------------------------------------------------------------------------------------------------- |
| `ok`              | True when setup and every callback succeeded.                                                     |
| `setup_failure`   | `FailureReport` when setup fails; otherwise `None`.                                               |
| `callbacks`       | Tuple of `CallbackResult` records for matching callbacks that ran, stopping at the first failure. |

Each `CallbackResult` describes one invocation:

| Field                                  | Meaning                                       |
| -------------------------------------- | --------------------------------------------- |
| `event_id`, `plugin_id`, `callback_id` | Event and callback identity.                  |
| `delivery_attempt`                     | One-based attempt number.                     |
| `failure`                              | `FailureReport` on failure; otherwise `None`. |

| `FailureReport` field                       | Meaning                                                                                                              |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `code`                                      | Stable failure code. Harness codes include `secret-missing`, `event-invalid`, `setup-failed`, and `callback-failed`. |
| `detail`                                    | Redacted detail including the code and identity, not just the underlying exception.                                  |
| `site`, `group`, `plugin_id`, `callback_id` | Identity of the failure.                                                                                             |

### Harness rules

- Each synthetic event must contain `type` and these fields: `id`, `event_type`, `attribute_name`,
  `meta`, `entity`, `user`, `project`, `session_uuid`, and `created_at`. A missing field, a
  matching event whose `id` is not a positive integer, or an unsupported or non-finite value in any
  field raises `ValueError`.
- The harness creates no SQLite database, source poller, supervisor, or real SG request. Its
  default handle rejects SG requests; pass `sg=handle` to supply a test handle with the methods
  your plugin calls, plus `set_session_uuid` and `close`. `set_session_uuid` is synchronous in both
  modes. For an async plugin, `close` must be async; for a sync plugin, it must be sync. A mismatch
  raises `ValueError` before setup. Async callable objects and decorators that preserve
  `__wrapped__` are recognized; an unmarked sync function returning a coroutine is outside this
  preflight check.
- Once setup acquires the handle, the harness owns it and closes it after success, an empty event
  sequence, or a setup or callback failure. A failure to close is raised rather than returned in
  the result: an ordinary exception becomes a `PluginError` with code `teardown-failed`. Import
  `PluginError` from `sg_event_daemon.testing` to inspect its `code`. When invalid input already
  raised an error, an ordinary teardown failure is attached as a redacted note to that error.
