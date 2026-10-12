# Development

## Local environment

Install these tools before you set up a checkout:

- Python 3.11 or newer linked to SQLite 3.51.3 or newer, as `python` on `PATH`. To use another
  interpreter, run `make setup PYTHON=/path/to/python`; setup never downloads Python.
- uv 0.12.13. Setup and the lock targets refuse any other version, so dependency locks resolve the
  same way locally and in CI.
- Node.js on `PATH` for Pyright. CI pins Node.js 24.21.0.
- Taplo 0.10.0 and shfmt 3.14.1. The Markdown formatter uses them for TOML and shell code blocks in
  the docs.

Then, from the repository root:

```sh
make setup
make check
```

`make setup` creates `.venv` if it does not exist, installs the exact hashed pins from
`requirements-ci.txt`, and installs the project in editable mode. Run it again after changing
`pyproject.toml` or the lock files, including when `make format` rewrites `pyproject.toml`. The
other targets compare `.venv` with those files and refuse to run when it is missing or outdated, so
a check never installs or updates packages.

`make quality` runs the read-only checks that CI's quality job runs: formatting and lint,
dependency locks, Pyright type checks for Linux and macOS, TOML test fixtures, and trailing
whitespace. `make check` adds the test suite, and `make test` runs the suite alone. `make format`
applies the formatters; review its changes before committing. `make install` installs the
`sg-event-daemon` command with `uv tool` and the default `shotgun-api3` provider extra;
`make reinstall` replaces that installation.

`requirements-ci.txt` pins the development tools, the default `shotgun-api3` provider extra, and
the build backend. `requirements-test.txt` selects the smaller set that test jobs install,
constrained by the same pins. Neither file ships with the package; published dependency ranges stay
in `pyproject.toml`.

CI downloads checksum-verified Linux builds of Taplo and shfmt. The lock leaves out the Python
Taplo package that `mdformat-config` would otherwise pull in, because that package lacks some ARM
wheels and CI uses the native binary instead. When Taplo or shfmt is unavailable, mdformat warns
that it could not format a code block but still succeeds, so read its warnings.

## Storage contention gate

Run `make benchmark-storage DEVICE_CLASS=... FILESYSTEM=... WORK_DIR=... RESULTS_DIR=...` on an
otherwise idle host. It seeds two populations and runs twelve cells, taking about 2.5 hours.
`WORK_DIR` must be on the filesystem being measured. It must be a non-symlink directory owned by
you with mode 0700 (created if missing), under a parent owned by you or root that others cannot
write unless it has the sticky bit. The command exits 0 only when every gated cell passes.

## Continuous integration

The [CI workflow](../.github/workflows/ci.yml) runs `quality` first. The `test` jobs then run in
parallel. `package` starts only after those jobs pass; `hosted-validation` reports the outcome of a
hosted run, including failures.

The quality job provisions Node.js 24.21.0 and checks Node and Pyright startup before running
quality checks. Test jobs do not invoke Pyright.

CI uses GitHub-hosted runners unless a run explicitly requests local runners. In a private
repository, the Actions variables `CI_LINUX_RUNNER` and `CI_MACOS_RUNNER` select local runners for
runs started from Actions > CI > Run workflow with the hosted option disabled. If a variable is
absent, that operating system uses a hosted runner. Pushes, pull requests, public repositories, and
runs with the hosted option enabled use hosted runners for every job and run every test.

A staging repository sets the Actions variable `CI_RUN_ON_PUSH` to `false`. Its pushes then appear
as `CI / push skipped` and run no jobs; CI runs only on request. A newer requested run on a branch
cancels an older one. Push runs are never cancelled, because the Release workflow needs the tagged
commit's push run.

| Requested run                                     | Actions > CI > Run workflow | Title               |
| ------------------------------------------------- | --------------------------- | ------------------- |
| Quick: local runners, slow tests omitted on macOS | hosted off, slow tests off  | `CI / local`        |
| Full: local runners, every test                   | hosted off, slow tests on   | `CI / local (full)` |
| Hosted: GitHub-hosted runners, every test         | hosted on                   | `CI / hosted`       |

The `test` matrix covers Python 3.11 to 3.15 on Linux and Python 3.13 on macOS. All test jobs have
a one-hour hang guard, on local and hosted runners, in quick and full runs.

Test jobs install uv-managed Python into the runner's temporary directory. The pinned uv catalog
provides builds with SQLite 3.53.1, above the daemon's 3.51.3 minimum for the WAL corruption fix.
Each job prints its Python and SQLite versions and checks the daemon's SQLite requirement before
running tests. The runner's system Python and SQLite installations are unchanged.

CI runs tests in parallel with pytest-xdist. Set `PYTEST_XDIST_AUTO_NUM_WORKERS` when a runner's
visible CPU count exceeds its CPU quota. For the same scheduling locally, run:

```sh
make test TEST_FLAGS='-n auto --dist worksteal --durations=15'
```

The manual [Release workflow](../.github/workflows/release.yml) publishes the package to PyPI after
checking its version tag and a successful push CI run for the tagged commit.

## Dependency updates

Use the uv version pinned in CI. After changing dependencies, run `make lock-update`. For an
intentional upgrade, run `make lock-update LOCK_FLAGS=--upgrade` and review both pin diffs. Run
`make lock-check` to verify them; `make check` includes this check.

## Project notes

Exec-spawned storage tests that exercise process death, worker contention, and checkpoints carry
the `slow` marker. The quick local macOS CI mode skips them; the full local and hosted modes run
them, as does Linux CI. Locally, run all tests with `make test`, or skip slow tests with
`make test TEST_FLAGS='-m "not slow"'`. Database and socket fixtures use per-test temporary paths;
listeners choose ephemeral ports. The [storage contention gate](#storage-contention-gate) is a
separate measurement procedure.

### Live tests

Tests marked `live` and `live_load` run the daemon against a real SG site. They are skipped unless
you pass `--run-live` or `--run-live-load`, together with `--live-provider` (the installed
provider, such as `shotgun-api3`) and `--live-mode` (`sync`, or `sync+async` with the async
provider):

```sh
.venv/bin/python -m pytest -q --run-live --live-provider=shotgun-api3 --live-mode=sync
```

The tests read `SG_TEST_SITE` and `SG_TEST_PROJECT`, an event-reading script in
`SG_EVENT_TEST_SOURCE_SCRIPT` and `SG_EVENT_TEST_SOURCE_KEY`, and a script that makes the test
changes in `SG_EVENT_TEST_PRODUCER_SCRIPT` and `SG_EVENT_TEST_PRODUCER_KEY`.
`SG_EVENT_TEST_CLEANUP_SCRIPT` and `SG_EVENT_TEST_CLEANUP_KEY` optionally supply a separate cleanup
credential. Because the producer changes records in that project, use a test site. For the load
test, `--live-load-events` sets the event count (2 through 1000, default 1000), and
`--live-report-dir` writes a sanitized report.
