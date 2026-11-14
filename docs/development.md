# Development

Use this guide to set up a development checkout, run checks, and update dependencies. For writing
and testing plugins, see [Porting plugins](porting.md).

## Set up a development checkout

Install these tools before running setup:

| Tool    | Requirement                                                                       |
| ------- | --------------------------------------------------------------------------------- |
| Python  | 3.11 or newer, linked to SQLite 3.51.3 or newer, available as `python` on `PATH`. |
| uv      | 0.13.6. Setup and lock targets require this version.                              |
| Node.js | Available on `PATH` for Pyright.                                                  |
| Taplo   | 0.10.0, for TOML code blocks in Markdown.                                         |
| shfmt   | 3.14.1, for shell code blocks in Markdown.                                        |

From the repository root, run:

```sh
make setup
make check
```

`make setup` creates `.venv` if needed, installs the exact hashed pins from `requirements-ci.txt`,
and installs the project in editable mode. To choose an interpreter when creating `.venv`, use
`make setup PYTHON=/path/to/python`. If `.venv` already exists, `PYTHON` does not replace its
interpreter; remove `.venv` before running setup with another interpreter. Setup never downloads
Python. A successful `make check` means the read-only checks and offline tests passed.

Run setup again after changing `pyproject.toml` or the lock files, including when `make format`
rewrites `pyproject.toml`. Check targets refuse a missing or outdated environment; they never
install or update packages.

## Run checks

Choose the target for the work you are doing:

| Target           | What it does                                                                                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `make check`     | Runs quality checks and the offline test suite.                                                                                                         |
| `make quality`   | Checks formatting, lint, dependency locks, Linux and macOS types, TOML fixtures, whitespace, and GitHub Actions workflows with an offline zizmor audit. |
| `make test`      | Runs the offline test suite.                                                                                                                            |
| `make format`    | Applies formatters. Inspect its changes before committing.                                                                                              |
| `make install`   | Installs the command with `uv tool` and the default `shotgun-api3` extra.                                                                               |
| `make reinstall` | Replaces that tool installation.                                                                                                                        |

Taplo and shfmt must be available when formatting Markdown. If either is missing, mdformat warns
that it could not format a code block but can still succeed; check the warnings as well as the exit
status.

Storage tests that spawn processes for process death, worker contention, and checkpoints carry the
`slow` marker. `make test` includes them. For a shorter local run, skip them:

```sh
make test TEST_FLAGS='-m "not slow"'
```

For parallel scheduling like CI, run:

```sh
make test TEST_FLAGS='-n auto --dist worksteal --durations=15'
```

Set `PYTEST_XDIST_AUTO_NUM_WORKERS` when the visible CPU count exceeds the available CPU quota.
Database and socket fixtures use per-test temporary paths, and listeners use ephemeral ports. The
[storage contention gate](#storage-contention-gate) is a separate measurement procedure.

## Update dependencies

Declare package dependency ranges in `pyproject.toml`. `requirements-ci.txt` pins development
tools, the default `shotgun-api3` provider, and the build backend. `requirements-test.txt` selects
the smaller test-job environment, constrained by the same pins. Neither lock file ships with the
package.

Use the pinned uv version. After changing dependencies, update both locks and check the result:

```sh
make lock-update
make setup
make lock-check
```

For an intentional dependency upgrade, use `make lock-update LOCK_FLAGS=--upgrade`. Review both pin
diffs. `make check` also checks the locks.

## Continuous integration

The [CI workflow](../.github/workflows/ci.yml) runs `quality` first, then runs the `test` jobs in
parallel. `package` starts after they pass; `hosted-validation` reports the overall outcome,
including failures. By default, pushes run every job on GitHub-hosted runners with every offline
test. Setting `CI_RUN_ON_PUSH=false` skips push runs. Manual dispatch can select hosted runners or,
in a private repository with configured runners, local runners; `full_tests` includes slow tests on
local runners.

The quality job provisions Node.js and checks Node and Pyright startup before running quality
checks. It installs checksum-verified native Linux builds of Taplo and shfmt. Test jobs do not
invoke Pyright.

The `test` matrix covers Python 3.11 to 3.15 on Linux and Python 3.13 on macOS. All test jobs have
a one-hour hang guard.

Test jobs install uv-managed Python into the runner's temporary directory and check its SQLite
version against the daemon's minimum before running tests. Each job prints its Python and SQLite
versions. The runner's system installations are unchanged.

The wheel contains the runtime package. The source distribution contains the README, package
source, and required build and license files. Documentation, benchmarks, and tests remain available
in the Git checkout.

## Run live tests

Live tests change records in SG. Use a test site and project, and supply these environment
variables before running them:

| Variables                                                     | Purpose                                    |
| ------------------------------------------------------------- | ------------------------------------------ |
| `SG_TEST_SITE`, `SG_TEST_PROJECT`                             | Test site and project.                     |
| `SG_EVENT_TEST_SOURCE_SCRIPT`, `SG_EVENT_TEST_SOURCE_KEY`     | Script credential that reads events.       |
| `SG_EVENT_TEST_PRODUCER_SCRIPT`, `SG_EVENT_TEST_PRODUCER_KEY` | Script credential that makes test changes. |
| `SG_EVENT_TEST_CLEANUP_SCRIPT`, `SG_EVENT_TEST_CLEANUP_KEY`   | Optional separate cleanup credential.      |

Tests marked `live` or `live_load` are skipped unless you pass `--run-live` or `--run-live-load`.
Also select the installed provider with `--live-provider` and its execution modes with
`--live-mode`: `sync`, or `sync+async` for the async provider. For example:

```sh
.venv/bin/python -m pytest -q --run-live --live-provider=shotgun-api3 --live-mode=sync
```

| Load-test option     | Meaning                                              |
| -------------------- | ---------------------------------------------------- |
| `--live-load-events` | Number of events, from 2 through 1000; default 1000. |
| `--live-report-dir`  | Directory for a sanitized report.                    |

### Read live load reports

Load reports distinguish two latency views for each expected observer-late event:

| View                         | Meaning                                                                                                                                                                                                           |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Observer-to-eager            | End-to-end delay, including discovery and recheck issuance before the source returns the event. The ten-second comparison is descriptive.                                                                         |
| First-source-return-to-eager | Time from the first successful source event-body read returning the ID to its eager callback. Missing-ID and deadline rechecks count as source reads; recovery requests after that return count toward the delay. |

The `eager_daemon_latency` criterion requires one eager callback and attributable first source
return per expected late ID, with no measured daemon share above ten seconds. A callback before the
first return has zero remaining share; its signed timing remains in the event record. The summary
includes measured and missing-evidence counts, median, mean, maximum, and count above ten seconds.
Strict delivery must be once per expected event and in order, but has no latency bound.

Reports also include the slow-site flag, capacity detach counts and entries, and the peak count of
returned but undelivered expected IDs. That last count describes pending delivery, not channel
queue occupancy. Visibility reports list skipped ID ranges separately from observed IDs when a
higher event has remained visible beyond the 30-second bulk window. A slow-site flag alone does not
pass or fail the test.

## Delivery and storage benchmarks

Use `make benchmark-delivery WORK_DIR="$WORK_DIR"` for a dedicated worker burst. `GROUPS` selects
`alone` (the default) or `paired`, and `BURST` sets the event count (default 10,000).

Use `make benchmark-load` for sustained source writes and lane delivery. Set `DEVICE_CLASS`,
`FILESYSTEM`, `WORK_DIR`, and `RESULTS_DIR` as for the
[storage contention gate](#storage-contention-gate). `FLOORS` selects `gate` (the default) or
`report`; pass extra benchmark arguments through `BENCHMARK_FLAGS`.

## Storage contention gate

Run this gate on an otherwise idle host. Put `WORK_DIR` on the filesystem you want to measure. It
must be a non-symlink directory owned by you with mode `0700`; the command creates it if missing.
Its parent must be owned by you or root and not writable by others unless it has the sticky bit.

```sh
make benchmark-storage DEVICE_CLASS="$DEVICE_CLASS" FILESYSTEM="$FILESYSTEM" \
    WORK_DIR="$WORK_DIR" RESULTS_DIR="$RESULTS_DIR"
```

The command seeds two populations and runs twelve cells, taking about 2.5 hours. It exits 0 only
when every gated cell passes.

## Host probes

Measure the configured IPC frame capacity and cross-process monotonic clock agreement on the
benchmark host:

```sh
.venv/bin/python benchmarks/host_probe.py socket-capacity --max-frame-bytes 8388608
.venv/bin/python benchmarks/host_probe.py monotonic --processes 8 --rounds 1000
```

Use your configured `max_ipc_frame_bytes` value in the first command; 8,388,608 bytes is the
default. Both commands print one JSON result. The monotonic command exits nonzero if a child clock
reading falls outside its parent sample. Run both probes inside the intended container as well. The
socket probe imports the daemon package from the mounted source tree:

```sh
docker run --rm -v "$PWD:/repo:ro" -w /repo -e PYTHONPATH=/repo/src \
    public.ecr.aws/docker/library/python:3.13-slim \
    python benchmarks/host_probe.py socket-capacity --max-frame-bytes 8388608
docker run --rm -v "$PWD:/repo:ro" -w /repo \
    public.ecr.aws/docker/library/python:3.13-slim \
    python benchmarks/host_probe.py monotonic --processes 8 --rounds 1000
```
