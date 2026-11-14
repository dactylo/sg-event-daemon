.DEFAULT_GOAL := all

# Every file git tracks or would add, except paths git marks `private`.
git_files = $(wildcard $(shell git -c core.quotePath=false ls-files --cached --others --exclude-standard -- $(1) ':(exclude,attr:private)'))
MARKDOWN_FILES = $(call git_files,'*.md')
PYTHON_FILES = $(call git_files,'*.py' '*.pyi')
WORKFLOW_FILES := $(wildcard .github/workflows/*.yml)
TOML_FILES := tests/fixtures/valid.toml

PYTHON ?= python
FLOORS ?= gate
GROUPS ?= alone
BURST ?= 10000
VENV_BIN := .venv/bin
DEV_PYTHON := $(VENV_BIN)/python
export PATH := $(abspath $(VENV_BIN)):$(PATH)
export UV_SYSTEM_PYTHON := 0
UV_VERSION := 0.13.6
CI_PYTHON := 3.11
CI_EXTRAS := --extra shotgun-api3
CI_BUILD_REQUIREMENTS = $(DEV_PYTHON) -c 'import tomllib; print("\n".join(tomllib.load(open("pyproject.toml", "rb"))["build-system"]["requires"]))'
CI_COMPILE = uv pip compile pyproject.toml - $(CI_EXTRAS) --universal --python-version $(CI_PYTHON) --no-python-downloads --no-sources --no-build --generate-hashes --no-annotate --custom-compile-command 'make lock-update'

.PHONY: all benchmark-delivery benchmark-load benchmark-storage check env-check file-check format format-check install lock-check lock-update quality reinstall setup test toml-check type-check uv-check venv-check whitespace-check workflow-check

uv-check:
	@test "$$(uv --version | cut -d ' ' -f 2)" = "$(UV_VERSION)" || \
		{ echo 'Use uv $(UV_VERSION), matching CI.' >&2; exit 1; }

setup: uv-check
	@test -d .venv || uv venv --python "$(PYTHON)" --no-python-downloads .venv
	rm -f .venv/.requirements-ci.txt .venv/.pyproject.toml
	uv pip sync --python "$(DEV_PYTHON)" --no-python-downloads --require-hashes --build-constraints requirements-ci.txt requirements-ci.txt
	uv pip install --python "$(DEV_PYTHON)" --no-python-downloads --no-deps --no-build-isolation -e .
	cp requirements-ci.txt .venv/.requirements-ci.txt
	cp pyproject.toml .venv/.pyproject.toml

venv-check:
	@test -x "$(DEV_PYTHON)" || { echo 'Run make setup before project commands.' >&2; exit 1; }

env-check: venv-check
	@cmp -s requirements-ci.txt .venv/.requirements-ci.txt && \
		cmp -s pyproject.toml .venv/.pyproject.toml || \
		{ echo 'Project setup is incomplete or outdated; run make setup.' >&2; exit 1; }

# A tool given no files, or a folder, searches folders itself; stop before that can happen.
file-check:
	@test -n "$(PYTHON_FILES)" && test -n "$(MARKDOWN_FILES)" || \
		{ echo 'git listed no files to check; run make from a git checkout.' >&2; exit 1; }
	@! git ls-files --cached --others --exclude-standard -- '*.py' '*.pyi' '*.md' ':(exclude,attr:private)' | grep -q '[[:space:]]' || \
		{ echo 'Rename Python and Markdown files whose names contain whitespace.' >&2; exit 1; }

install:
	uv tool install '.[shotgun-api3]'

reinstall:
	uv tool install --reinstall '.[shotgun-api3]'

format: env-check file-check
	$(DEV_PYTHON) -m ruff format $(PYTHON_FILES)
	$(DEV_PYTHON) -m ruff check --fix $(PYTHON_FILES) pyproject.toml
	$(DEV_PYTHON) -m mdformat $(MARKDOWN_FILES)
	$(VENV_BIN)/pyproject-fmt pyproject.toml

format-check: env-check file-check
	$(DEV_PYTHON) -m ruff format --check $(PYTHON_FILES)
	$(DEV_PYTHON) -m ruff check $(PYTHON_FILES) pyproject.toml
	$(DEV_PYTHON) -m mdformat --check $(MARKDOWN_FILES)
	$(VENV_BIN)/pyproject-fmt --check pyproject.toml

lock-check: env-check uv-check
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	cp requirements-ci.txt "$$tmp/requirements-ci.txt" && \
	$(CI_BUILD_REQUIREMENTS) | $(CI_COMPILE) --group dev --quiet --output-file "$$tmp/requirements-ci.txt" && \
	diff -u requirements-ci.txt "$$tmp/requirements-ci.txt" && \
	$(CI_BUILD_REQUIREMENTS) | $(CI_COMPILE) --group test --constraint "$$tmp/requirements-ci.txt" --quiet --output-file "$$tmp/requirements-test.txt" && \
	diff -u requirements-test.txt "$$tmp/requirements-test.txt"

lock-update: venv-check uv-check
	$(CI_BUILD_REQUIREMENTS) | $(CI_COMPILE) --group dev --output-file requirements-ci.txt $(LOCK_FLAGS)
	$(CI_BUILD_REQUIREMENTS) | $(CI_COMPILE) --group test --constraint requirements-ci.txt --output-file requirements-test.txt

type-check: env-check file-check
	$(DEV_PYTHON) -m pyright --project pyproject.toml --pythonpath "$(DEV_PYTHON)" --pythonplatform Linux $(PYTHON_FILES)
	$(DEV_PYTHON) -m pyright --project pyproject.toml --pythonpath "$(DEV_PYTHON)" --pythonplatform Darwin $(PYTHON_FILES)

test: env-check
	$(DEV_PYTHON) -m pytest -q $(TEST_FLAGS)

toml-check: env-check
	$(DEV_PYTHON) -c 'import pathlib, sys, tomllib; [tomllib.loads(pathlib.Path(path).read_text(encoding="utf-8")) for path in sys.argv[1:]]' $(TOML_FILES)


whitespace-check:
	git diff --check "$$(git hash-object -t tree /dev/null)" -- . ':(exclude,attr:private)'

benchmark-delivery: env-check
	@test -n "$(WORK_DIR)" || { echo 'Set WORK_DIR.' >&2; exit 1; }
	$(DEV_PYTHON) -m benchmarks.delivery_speed --groups "$(GROUPS)" --work-dir "$(WORK_DIR)" --burst "$(BURST)"

benchmark-storage: env-check
	@test -n "$(DEVICE_CLASS)" && test -n "$(FILESYSTEM)" && test -n "$(WORK_DIR)" && test -n "$(RESULTS_DIR)" || \
		{ echo 'Set DEVICE_CLASS, FILESYSTEM, WORK_DIR, and RESULTS_DIR.' >&2; exit 1; }
	$(DEV_PYTHON) benchmarks/storage_contention.py gate --device-class "$(DEVICE_CLASS)" --filesystem "$(FILESYSTEM)" --work-dir "$(WORK_DIR)" --results-dir "$(RESULTS_DIR)" --commit "$$(git rev-parse HEAD)" $(BENCHMARK_FLAGS)

benchmark-load: env-check
	@test -n "$(DEVICE_CLASS)" && test -n "$(FILESYSTEM)" && test -n "$(WORK_DIR)" && test -n "$(RESULTS_DIR)" || \
		{ echo 'Set DEVICE_CLASS, FILESYSTEM, WORK_DIR, and RESULTS_DIR.' >&2; exit 1; }
	$(DEV_PYTHON) -m benchmarks.storage_load run --cell all --device-class "$(DEVICE_CLASS)" --filesystem "$(FILESYSTEM)" --work-dir "$(WORK_DIR)" --results-dir "$(RESULTS_DIR)" --commit "$$(git rev-parse HEAD)" --floors "$(FLOORS)" $(BENCHMARK_FLAGS)

workflow-check: env-check
	$(VENV_BIN)/zizmor --offline --no-progress --quiet $(WORKFLOW_FILES)

quality: format-check lock-check type-check toml-check whitespace-check workflow-check

check: quality test

all: check
