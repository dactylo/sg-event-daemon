from __future__ import annotations

import ipaddress
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import cast

import pytest

from sg_event_daemon._config import (
    AcceptanceEvidence,
    AcceptanceSourceEvidence,
    Config,
    ConfigError,
    CredentialConfig,
    DaemonConfig,
    GroupConfig,
    SecretRef,
    SecretValue,
    SourceConfig,
    WorkerConfig,
    _strict_group_holdback_secs,
    evidence_warnings,
    load_acceptance_evidence,
    load_config,
    load_control_config,
    source_registration_holdback_secs,
    source_stream_fingerprint,
)
from sg_event_daemon._identity import stream_fingerprint

_COUNT_FIELDS = (
    ('daemon', 'max_concurrent_catchup_readers'),
    ('daemon', 'max_failure_text_bytes'),
    ('daemon', 'max_group_obligations'),
    ('daemon', 'max_ipc_frame_bytes'),
    ('daemon', 'max_manifest_bytes'),
    ('daemon', 'max_pending_epochs'),
    ('daemon', 'max_plugin_config_bytes'),
    ('credential', 'max_rpc_attempts'),
    ('source', 'batch_size'),
    ('source', 'commit_batch_max_bytes'),
    ('source', 'max_event_bytes'),
    ('source', 'max_late_ids_per_group'),
    ('source', 'max_open_ranges'),
    ('source', 'max_pending_group_obligations'),
    ('source', 'max_range_query_pages'),
    ('source', 'max_source_late_ids'),
    ('source', 'queue_max_bytes'),
    ('source', 'queue_max_events'),
    ('source', 'strict_max_bytes'),
    ('source', 'strict_max_events'),
    ('worker', 'restart_limit'),
)
_DURATION_FIELDS = (
    ('credential', 'rpc_attempt_interval_secs'),
    ('credential', 'timeout_secs'),
    ('daemon', 'async_cancel_grace_secs'),
    ('daemon', 'checkpoint_interval_secs'),
    ('daemon', 'health_interval_secs'),
    ('daemon', 'shutdown_deadline_secs'),
    ('daemon', 'supervisor_poll_interval_secs'),
    ('daemon', 'worker_startup_deadline_secs'),
    ('group', 'callback_deadline_secs'),
    ('group', 'strict_holdback_secs'),
    ('source', 'fetch_interval_secs'),
    ('source', 'poll_retry_base_secs'),
    ('source', 'poll_retry_max_secs'),
    ('source', 'visibility_overlap_secs'),
    ('worker', 'healthy_reset_secs'),
    ('worker', 'restart_window_secs'),
)
_FIXTURES = Path(__file__).parent / 'fixtures'
_LIMIT_RELATIONSHIP_CASES = (
    (
        'max_ipc_frame_bytes = 1116',
        'max_event_bytes = 1000',
        '',
        'max_event_bytes.*max_ipc_frame_bytes',
    ),
    (
        'max_ipc_frame_bytes = 1117',
        'max_event_bytes = 1000\nqueue_max_bytes = 1116\ncommit_batch_max_bytes = 1121',
        '',
        'queue_max_bytes.*max_ipc_frame_bytes',
    ),
    (
        '',
        'max_event_bytes = 1\ncommit_batch_max_bytes = 121',
        '',
        'commit_batch_max_bytes.*maximum framed event',
    ),
    (
        '',
        'max_event_bytes = 1\nstrict_max_bytes = 121',
        'ordering = "strict"',
        'strict_max_bytes.*maximum framed event',
    ),
    (
        'max_group_obligations = 1',
        'max_open_ranges = 2',
        '',
        'max_group_obligations.*max_open_ranges',
    ),
    (
        'max_pending_epochs = 1',
        '',
        '',
        'max_pending_epochs.*at least 2',
    ),
    (
        '',
        'poll_retry_base_secs = 2.0\npoll_retry_max_secs = 1.0',
        '',
        'poll_retry_max_secs.*poll_retry_base_secs',
    ),
    (
        '',
        'poll_retry_multiplier = 0.9',
        '',
        'poll_retry_multiplier.*at least 1',
    ),
    (
        '',
        'poll_retry_jitter_fraction = 1.0',
        '',
        'poll_retry_jitter_fraction.*\\[0, 1\\)',
    ),
    (
        '',
        'poll_retry_jitter_fraction = -0.1',
        '',
        'poll_retry_jitter_fraction.*\\[0, 1\\)',
    ),
)
_NON_JSON_VALUES = (
    '1979-05-27',
    '07:32:00',
    '1979-05-27T07:32:00',
    '1979-05-27T07:32:00Z',
    'nan',
    'inf',
    '-inf',
)


def _config_text(
    plugin_path: Path,
    **options: str,
) -> str:
    credential_extra = options.pop('credential_extra', '')
    daemon_extra = options.pop('daemon_extra', '')
    group_extra = options.pop('group_extra', '')
    plugin_config = options.pop('plugin_config', '')
    plugin_extra = options.pop('plugin_extra', '')
    plugin_secrets = options.pop('plugin_secrets', '')
    provider = options.pop('provider', 'shotgun-api3')
    source_extra = options.pop('source_extra', '')
    worker_table = options.pop('worker_table', '')
    if options:
        raise AssertionError(f'unknown config test options: {sorted(options)}')
    config_table = (
        f'\n[sites.test.groups.noop.plugins.config]\n{plugin_config}\n' if plugin_config else ''
    )
    secrets_table = (
        f'\n[sites.test.groups.noop.plugins.secrets]\n{plugin_secrets}\n' if plugin_secrets else ''
    )
    return f'''
[daemon]
state_dir = "/tmp/sg-event-daemon-test"
sg_api_provider = "{provider}"
{daemon_extra}

[sites.test]
server = "https://example.invalid"
default_source = "admin"

[sites.test.credentials.admin]
script_name = "test"
api_key = {{ env = "SG_EVENT_ADMIN_KEY" }}
{credential_extra}

[sites.test.sources.admin]
credentials = "admin"
{source_extra}

[sites.test.groups.noop]
{group_extra}

[[sites.test.groups.noop.plugins]]
id = "noop"
path = "{plugin_path}"
{plugin_extra}
{config_table}
{secrets_table}
{worker_table}
'''


def _four_site_text(plugin_path: Path) -> str:
    site_blocks = [
        f'''
[sites.{name}]
server = "https://{name}.example.invalid"
default_source = "events"

[sites.{name}.credentials.admin]
script_name = "{name}-admin"
api_key = {{ env = "SG_EVENT_ADMIN_KEY" }}

[sites.{name}.sources.events]
credentials = "admin"

[sites.{name}.groups.default]

[[sites.{name}.groups.default.plugins]]
id = "noop"
path = "{plugin_path}"
'''
        for name in ('beta', 'gamma', 'delta')
    ]
    return f'''
[daemon]
state_dir = "/var/lib/sg-event-daemon"
sg_api_provider = "shotgun-api3"

[sites.alpha]
server = "https://alpha.example.invalid"
default_source = "events"

[sites.alpha.credentials.admin]
script_name = "alpha-admin"
api_key = {{ env = "SG_EVENT_ADMIN_KEY" }}

[sites.alpha.credentials.editorial]
script_name = "alpha-editorial"
api_key = {{ env = "SG_EDITORIAL_KEY" }}

[sites.alpha.sources.events]
credentials = "admin"

[sites.alpha.workers.pool]

[sites.alpha.groups.eager]

[[sites.alpha.groups.eager.plugins]]
id = "first"
path = "{plugin_path}"

[sites.alpha.groups.strict]
callback_credentials = "editorial"
ordering = "strict"
worker = "pool"

[[sites.alpha.groups.strict.plugins]]
id = "first"
path = "{plugin_path}"

[[sites.alpha.groups.strict.plugins]]
id = "second"
path = "{plugin_path}"
credentials = "admin"
''' + ''.join(site_blocks)


def _numeric_config(
    plugin_path: Path,
    location: str,
    assignment: str,
) -> str:
    if location == 'credential':
        return _config_text(plugin_path, credential_extra=assignment)
    if location == 'daemon':
        return _config_text(plugin_path, daemon_extra=assignment)
    if location == 'group':
        return _config_text(
            plugin_path,
            group_extra=f'ordering = "strict"\n{assignment}',
        )
    if location == 'source':
        return _config_text(plugin_path, source_extra=assignment)
    if location == 'worker':
        worker_table = f"""
[sites.test.workers.pool]
{assignment}
"""
        return _config_text(plugin_path, worker_table=worker_table)
    raise AssertionError(f'unknown test location: {location}')


def _plugin_file(tmp_path: Path) -> Path:
    path = tmp_path / 'plugin.py'
    path.write_text('def register(reg):\n    pass\n', encoding='utf-8')
    return path


def _write(tmp_path: Path, text: str, *, name: str = 'config.toml') -> Path:
    path = tmp_path / name
    path.write_text(text, encoding='utf-8')
    return path


def test_acceptance_evidence_rejects_duplicate_source_pairs(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[[sources]]
site = "test"
source = "admin"
source_fingerprint = "abc"
captured_at = 2026-07-25T12:00:00Z
observed_gap_rate = 0.1

[[sources]]
site = "test"
source = "admin"
source_fingerprint = "def"
captured_at = 2026-07-25T13:00:00Z
observed_gap_rate = 0.2
""",
        name='evidence.toml',
    )

    with pytest.raises(
        ConfigError,
        match=r'duplicate acceptance evidence.*test/admin',
    ):
        load_acceptance_evidence(path)


@pytest.mark.parametrize(
    ('text', 'match'),
    [
        (
            """
[[sources]]
site = "test"
source = "admin"
source_fingerprint = "abc"
captured_at = 2026-07-25T12:00:00Z
observed_gap_rate = 0.1
unexpected = true
""",
            'unknown keys.*unexpected',
        ),
        (
            """
[[sources]]
site = "test"
source = "admin"
source_fingerprint = "abc"
captured_at = 2026-07-25T12:00:00
observed_gap_rate = 0.1
""",
            'captured_at.*offset',
        ),
        (
            """
[[sources]]
site = "test"
source = "admin"
source_fingerprint = "abc"
captured_at = 2026-07-25T12:00:00Z
observed_gap_rate = nan
""",
            'observed_gap_rate.*finite',
        ),
        (
            """
[[sources]]
site = "test"
source = "admin"
captured_at = 2026-07-25T12:00:00Z
observed_gap_rate = 0.1
""",
            'missing keys.*source_fingerprint',
        ),
    ],
)
def test_acceptance_evidence_rejects_malformed_records(
    text: str,
    match: str,
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError, match=match):
        load_acceptance_evidence(_write(tmp_path, text, name='evidence.toml'))


def test_acceptance_evidence_round_trips_with_optional_incarnation(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[[sources]]
site = "test"
source = "admin"
source_fingerprint = "abc123"
captured_at = 2026-07-25T12:00:00Z
observed_gap_rate = 0.125

[[sources]]
site = "other"
source = "events"
source_fingerprint = "def456"
stream_incarnation = "incarnation-1"
captured_at = 2026-07-25T13:00:00-07:00
observed_gap_rate = 0.25
""",
        name='evidence.toml',
    )

    evidence = load_acceptance_evidence(path)

    assert evidence == AcceptanceEvidence(
        sources=(
            AcceptanceSourceEvidence(
                site='test',
                source='admin',
                source_fingerprint='abc123',
                stream_incarnation=None,
                captured_at=datetime.fromisoformat('2026-07-25T12:00:00+00:00'),
                observed_gap_rate=0.125,
            ),
            AcceptanceSourceEvidence(
                site='other',
                source='events',
                source_fingerprint='def456',
                stream_incarnation='incarnation-1',
                captured_at=datetime.fromisoformat('2026-07-25T13:00:00-07:00'),
                observed_gap_rate=0.25,
            ),
        )
    )


def test_api_key_rotation_preserves_resolved_source_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plugin_path = _plugin_file(tmp_path)
    path = _write(
        tmp_path,
        _config_text(plugin_path, group_extra='ordering = "strict"'),
    )
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'first-secret')
    first = load_config(path)
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'rotated-secret')
    rotated = load_config(path)
    first_site = first.sites['test']
    rotated_site = rotated.sites['test']
    first_credential = first_site.credentials['admin']
    rotated_credential = rotated_site.credentials['admin']
    evidence = AcceptanceEvidence(
        sources=(
            AcceptanceSourceEvidence(
                site='test',
                source='admin',
                source_fingerprint=stream_fingerprint(
                    'https://example.invalid',
                    'admin',
                    'test',
                ),
                stream_incarnation=None,
                captured_at=datetime.fromisoformat('2026-07-25T12:00:00+00:00'),
                observed_gap_rate=0.125,
            ),
        )
    )
    expected = ('strict source test/admin observed gap rate 0.125',)

    expected_fingerprint = 'aae38800a33580c333a7efee2a7ca91f2a5d64ac9195a598fee9bdbcec590f1f'
    assert (
        source_stream_fingerprint(first_site, first_site.sources['admin']) == expected_fingerprint
    )
    assert (
        source_stream_fingerprint(rotated_site, rotated_site.sources['admin'])
        == expected_fingerprint
    )
    assert first_credential.api_key.reveal() == 'first-secret'
    assert rotated_credential.api_key.reveal() == 'rotated-secret'
    assert first_credential.api_key.reveal() != rotated_credential.api_key.reveal()
    first_warnings = evidence_warnings(first, evidence)
    rotated_warnings = evidence_warnings(rotated, evidence)
    assert first_warnings == expected
    assert rotated_warnings == expected
    assert all('stale' not in warning for warning in (*first_warnings, *rotated_warnings))


def test_callback_deadline_below_provider_budget_warns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    text = _config_text(
        plugin_path,
        credential_extra=(
            'timeout_secs = 10.0\nmax_rpc_attempts = 3\nrpc_attempt_interval_secs = 2.0'
        ),
        group_extra='callback_deadline_secs = 33.0',
    )

    with pytest.warns(UserWarning, match='callback deadline 33.0.*provider budget 34.0'):
        load_config(_write(tmp_path, text))


def test_committed_fixture_resolves_relative_plugin_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'fixture-secret')

    config = load_config(_FIXTURES / 'valid.toml')

    plugin = config.sites['test'].groups['noop'].plugins[0]
    assert plugin.path == (_FIXTURES / 'noop_plugin.py').resolve()


def test_config_type_is_immutable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    config = load_config(_write(tmp_path, _config_text(plugin_path)))

    assert isinstance(config, Config)
    assert isinstance(config.sites, MappingProxyType)
    set_config_attribute = config.__setattr__
    with pytest.raises(FrozenInstanceError):
        set_config_attribute('sites', {})


def test_cross_site_source_references_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    text = (
        _config_text(plugin_path, group_extra='source = "remote"')
        + f'''
[sites.other]
server = "https://other.example.invalid"
default_source = "remote"

[sites.other.credentials.admin]
script_name = "other"
api_key = {{ env = "SG_EVENT_ADMIN_KEY" }}

[sites.other.sources.remote]
credentials = "admin"

[sites.other.groups.noop]

[[sites.other.groups.noop.plugins]]
id = "noop"
path = "{plugin_path}"
'''
    )

    with pytest.raises(ConfigError, match=r'site test.*unknown source remote'):
        load_config(_write(tmp_path, text))


def test_dataclass_serialization_preserves_secret_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'credential-secret')
    plugin_path = _plugin_file(tmp_path)
    config = load_config(_write(tmp_path, _config_text(plugin_path)))
    credential = config.sites['test'].credentials['admin']

    serialized = asdict(credential)

    serialized_key = serialized['api_key']
    assert 'credential-secret' not in repr(serialized)
    assert isinstance(serialized_key, SecretValue)
    assert serialized_key.reveal() == 'credential-secret'


def test_directory_plugin_paths_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')

    with pytest.raises(ConfigError, match=r'plugin path.*regular file'):
        load_config(_write(tmp_path, _config_text(tmp_path)))


def test_duplicate_effective_polling_identities_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    text = (
        _config_text(plugin_path)
        + """
[sites.test.credentials.alias]
script_name = "test"
api_key = { env = "SG_EVENT_ADMIN_KEY" }

[sites.test.sources.alias]
credentials = "alias"
"""
    )

    with pytest.raises(ConfigError, match=r'duplicate polling identity.*test'):
        load_config(_write(tmp_path, text))


def test_duplicate_plugin_ids_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    text = (
        _config_text(plugin_path)
        + f'''
[[sites.test.groups.noop.plugins]]
id = "noop"
path = "{plugin_path}"
'''
    )

    with pytest.raises(ConfigError, match=r'duplicate plugin id.*noop'):
        load_config(_write(tmp_path, text))


def test_eager_groups_reject_strict_holdback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=r'strict_holdback_secs.*strict'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, group_extra='strict_holdback_secs = 5.0'),
            )
        )


def test_evidence_uses_runtime_fingerprint_independent_of_provider_and_tuning(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    baseline = load_config(
        _write(
            tmp_path,
            _config_text(plugin_path, group_extra='ordering = "strict"'),
            name='baseline.toml',
        )
    )
    tuned = load_config(
        _write(
            tmp_path,
            _config_text(
                plugin_path,
                credential_extra='timeout_secs = 20.0',
                group_extra='ordering = "strict"',
                provider='sg-python-api',
                source_extra='batch_size = 250\nmax_open_ranges = 200',
            ),
            name='tuned.toml',
        )
    )
    fingerprint = stream_fingerprint(
        'https://example.invalid',
        'admin',
        'test',
    )
    evidence = AcceptanceEvidence(
        sources=(
            AcceptanceSourceEvidence(
                site='test',
                source='admin',
                source_fingerprint=fingerprint,
                stream_incarnation=None,
                captured_at=datetime.fromisoformat('2026-07-25T12:00:00+00:00'),
                observed_gap_rate=0.125,
            ),
        )
    )

    assert evidence_warnings(baseline, evidence) == (
        'strict source test/admin observed gap rate 0.125',
    )
    assert evidence_warnings(tuned, evidence) == (
        'strict source test/admin observed gap rate 0.125',
    )


def test_evidence_warnings_classify_missing_stale_and_supplied_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    config = load_config(
        _write(
            tmp_path,
            _config_text(plugin_path, group_extra='ordering = "strict"'),
        )
    )
    fingerprint = stream_fingerprint(
        'https://example.invalid',
        'admin',
        'test',
    )
    captured_at = datetime.fromisoformat('2026-07-25T12:00:00+00:00')

    missing = evidence_warnings(config, AcceptanceEvidence(sources=()))
    stale = evidence_warnings(
        config,
        AcceptanceEvidence(
            sources=(
                AcceptanceSourceEvidence(
                    site='test',
                    source='admin',
                    source_fingerprint='stale',
                    stream_incarnation=None,
                    captured_at=captured_at,
                    observed_gap_rate=0.5,
                ),
            )
        ),
    )
    supplied = evidence_warnings(
        config,
        AcceptanceEvidence(
            sources=(
                AcceptanceSourceEvidence(
                    site='test',
                    source='admin',
                    source_fingerprint=fingerprint,
                    stream_incarnation=None,
                    captured_at=captured_at,
                    observed_gap_rate=0.125,
                ),
            )
        ),
    )

    assert missing == ('missing evidence for strict source test/admin',)
    assert stale == ('stale evidence for strict source test/admin',)
    assert supplied == ('strict source test/admin observed gap rate 0.125',)


def test_evidence_warnings_ignore_eager_only_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    config = load_config(_write(tmp_path, _config_text(plugin_path)))

    assert evidence_warnings(config, AcceptanceEvidence(sources=())) == ()


def test_explicit_defaults_resolve_like_omitted_and_direct_construction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    omitted = load_config(
        _write(
            tmp_path,
            _config_text(plugin_path, group_extra='ordering = "strict"'),
            name='omitted.toml',
        )
    )
    explicit = load_config(
        _write(
            tmp_path,
            _config_text(
                plugin_path,
                daemon_extra='max_pending_epochs = 8\nhealth_interval_secs = 2.0',
                credential_extra=(
                    'timeout_secs = 30.0\nmax_rpc_attempts = 3\nrpc_attempt_interval_secs = 3.0'
                ),
                source_extra='visibility_overlap_secs = 300.0\nqueue_max_events = 1000',
                group_extra='ordering = "strict"\nstrict_holdback_secs = 300.0',
                worker_table='[sites.test.workers.pool]\nrestart_limit = 5',
            ),
            name='explicit.toml',
        )
    )
    omitted_site = omitted.sites['test']
    explicit_site = explicit.sites['test']
    direct_credential = CredentialConfig('admin', 'test', SecretValue('secret'))
    direct_source = SourceConfig('admin', 'admin')
    direct_group = GroupConfig('noop', 'admin', (), 'admin', ordering='strict')
    direct_worker = WorkerConfig('pool')
    direct_daemon = DaemonConfig(tmp_path / 'state', 'shotgun-api3')

    assert (
        (
            omitted.daemon.max_pending_epochs,
            omitted.daemon.health_interval_secs,
        )
        == (
            explicit.daemon.max_pending_epochs,
            explicit.daemon.health_interval_secs,
        )
        == (direct_daemon.max_pending_epochs, direct_daemon.health_interval_secs)
        == (8, 2.0)
    )
    assert (
        (
            omitted_site.credentials['admin'].timeout_secs,
            omitted_site.credentials['admin'].max_rpc_attempts,
            omitted_site.credentials['admin'].rpc_attempt_interval_secs,
        )
        == (
            explicit_site.credentials['admin'].timeout_secs,
            explicit_site.credentials['admin'].max_rpc_attempts,
            explicit_site.credentials['admin'].rpc_attempt_interval_secs,
        )
        == (
            direct_credential.timeout_secs,
            direct_credential.max_rpc_attempts,
            direct_credential.rpc_attempt_interval_secs,
        )
        == (30.0, 3, 3.0)
    )
    assert (
        (
            omitted_site.sources['admin'].visibility_overlap_secs,
            omitted_site.sources['admin'].queue_max_events,
        )
        == (
            explicit_site.sources['admin'].visibility_overlap_secs,
            explicit_site.sources['admin'].queue_max_events,
        )
        == (direct_source.visibility_overlap_secs, direct_source.queue_max_events)
        == (300.0, 1_000)
    )
    assert (
        omitted_site.groups['noop'].strict_holdback_secs
        == explicit_site.groups['noop'].strict_holdback_secs
        == 300.0
    )
    assert direct_group.strict_holdback_secs is None
    assert (
        omitted_site.groups['noop'].worker
        == explicit_site.groups['noop'].worker
        == direct_group.worker
        == 'default'
    )
    assert explicit_site.workers['pool'].restart_limit == direct_worker.restart_limit == 5


def test_group_storage_holdback_stays_positive_when_eager_registration_is_zero() -> None:
    source = SourceConfig('events', 'source', visibility_overlap_secs=45.0)
    eager = GroupConfig(
        name='eager',
        source='events',
        plugins=(),
        callback_credentials='source',
    )
    strict = replace(eager, name='strict', ordering='strict')
    override = replace(strict, strict_holdback_secs=12.0)

    assert _strict_group_holdback_secs(eager, source) == 45.0
    assert source_registration_holdback_secs(eager, source) == 0.0
    assert _strict_group_holdback_secs(strict, source) == 45.0
    assert source_registration_holdback_secs(strict, source) == 45.0
    assert _strict_group_holdback_secs(override, source) == 12.0
    assert source_registration_holdback_secs(override, source) == 12.0


def test_load_config_parses_minimal_documented_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plugin_path = tmp_path / 'plugins' / 'shot_status.py'
    plugin_path.parent.mkdir()
    plugin_path.write_text('def register(reg):\n    pass\n', encoding='utf-8')
    monkeypatch.setenv('SG_EVENT_READER_KEY', 'reader-secret')
    monkeypatch.setenv('SG_EVENT_WRITER_KEY', 'writer-secret')
    path = _write(
        tmp_path,
        """[daemon]
state_dir = "/var/lib/sg-event-daemon"
sg_api_provider = "sg-python-api"

[sites.studio]
server = "https://studio.example.invalid"
default_source = "primary"

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
execution = "async"
ordering = "strict"

[[sites.studio.groups.editorial.plugins]]
id = "shot-status"
path = "plugins/shot_status.py"
credentials = "callback-writer"
config = { project_id = 123 }
""",
    )

    config = load_config(path)

    assert config.daemon.sg_api_provider == 'sg-python-api'
    site = config.sites['studio']
    assert site.default_source == 'primary'
    assert tuple(site.credentials) == ('event-reader', 'callback-writer')
    event_reader = site.credentials['event-reader']
    assert event_reader.script_name == 'event-daemon-reader'
    assert event_reader.api_key.reveal() == 'reader-secret'
    callback_writer = site.credentials['callback-writer']
    assert callback_writer.script_name == 'event-daemon-writer'
    assert callback_writer.api_key.reveal() == 'writer-secret'
    assert site.sources['primary'].credentials == 'event-reader'
    group = site.groups['editorial']
    assert group.source == 'primary'
    assert group.callback_credentials == 'callback-writer'
    assert group.execution == 'async'
    assert group.ordering == 'strict'
    plugin = group.plugins[0]
    assert plugin.id == 'shot-status'
    assert plugin.path == plugin_path.resolve()
    assert plugin.credentials == 'callback-writer'
    assert dict(plugin.config) == {'project_id': 123}


def test_load_config_resolves_defaults_references_and_plugin_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plugin_path = _plugin_file(tmp_path)
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'admin-secret')
    monkeypatch.setenv('SG_EDITORIAL_KEY', 'editorial-secret')

    config = load_config(_write(tmp_path, _four_site_text(plugin_path)))

    assert tuple(config.sites) == ('alpha', 'beta', 'gamma', 'delta')
    assert asdict(config.daemon) == {
        'async_cancel_grace_secs': 5.0,
        'checkpoint_interval_secs': 1.0,
        'health_interval_secs': 2.0,
        'max_concurrent_catchup_readers': 4,
        'max_failure_text_bytes': 262_144,
        'max_group_obligations': 1_000,
        'max_ipc_frame_bytes': 8_388_608,
        'max_manifest_bytes': 1_048_576,
        'max_pending_epochs': 8,
        'max_plugin_config_bytes': 262_144,
        'probe_bind': str(ipaddress.IPv4Address(0)),
        'probe_port': None,
        'sg_api_provider': 'shotgun-api3',
        'shutdown_deadline_secs': 10.0,
        'sqlite_synchronous': 'normal',
        'state_dir': Path('/var/lib/sg-event-daemon'),
        'supervisor_poll_interval_secs': 1.0,
        'worker_startup_deadline_secs': 30.0,
    }
    alpha = config.sites['alpha']
    admin = alpha.credentials['admin']
    assert admin.timeout_secs == 30.0
    assert admin.max_rpc_attempts == 3
    assert admin.rpc_attempt_interval_secs == 3.0
    source = alpha.sources['events']
    assert asdict(source) == {
        'batch_size': 500,
        'commit_batch_max_bytes': 67_108_864,
        'credentials': 'admin',
        'fetch_interval_secs': 1.0,
        'max_event_bytes': 4_194_304,
        'max_late_ids_per_group': 50_000,
        'max_open_ranges': 100,
        'max_pending_group_obligations': 1_300,
        'max_range_query_pages': 100,
        'max_source_late_ids': 50_000,
        'name': 'events',
        'poll_retry_base_secs': 1.0,
        'poll_retry_jitter_fraction': 0.2,
        'poll_retry_max_secs': 60.0,
        'poll_retry_multiplier': 2.0,
        'queue_max_bytes': 67_108_864,
        'queue_max_events': 1_000,
        'strict_max_bytes': 268_435_456,
        'strict_max_events': 20_000,
        'visibility_overlap_secs': 300.0,
    }
    worker = alpha.workers['pool']
    assert worker.restart_limit == 5
    assert worker.restart_window_secs == 300.0
    assert worker.healthy_reset_secs == 300.0
    eager = alpha.groups['eager']
    assert eager.source == 'events'
    assert eager.execution == 'sync'
    assert eager.ordering == 'eager'
    assert eager.strict_holdback_secs is None
    assert eager.worker == 'default'
    assert eager.callback_deadline_secs == 120.0
    assert alpha.groups['strict'].source == 'events'
    assert eager.callback_credentials == 'admin'
    assert eager.plugins[0].credentials == 'admin'
    assert alpha.groups['strict'].callback_credentials == 'editorial'
    assert tuple(plugin.id for plugin in alpha.groups['strict'].plugins) == ('first', 'second')
    assert tuple(plugin.credentials for plugin in alpha.groups['strict'].plugins) == (
        'editorial',
        'admin',
    )
    assert alpha.groups['strict'].strict_holdback_secs == 300.0
    assert alpha.groups['strict'].worker == 'pool'


def test_load_control_config_ignores_all_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('SG_EVENT_ADMIN_KEY', raising=False)

    control = load_control_config(_FIXTURES / 'valid.toml')

    assert control.state_dir.name == 'sg-event-daemon-test'
    assert control.state_dir.parent.name == 'tmp'


def test_load_control_config_wraps_malformed_toml(tmp_path: Path) -> None:
    path = _write(tmp_path, '[daemon\nstate_dir = "/tmp/state"')

    with pytest.raises(ConfigError, match='invalid TOML'):
        load_control_config(path)


def test_names_longer_than_64_characters_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    long_name = 'a' * 65
    text = _config_text(plugin_path).replace('[sites.test]', f'[sites.{long_name}]', 1)

    with pytest.raises(ConfigError, match='invalid site name'):
        load_config(_write(tmp_path, text))


def test_one_maximum_framed_event_fits_at_exact_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    config = load_config(
        _write(
            tmp_path,
            _config_text(
                plugin_path,
                daemon_extra='max_ipc_frame_bytes = 1117',
                source_extra=(
                    'max_event_bytes = 1000\nqueue_max_bytes = 1117\n'
                    'commit_batch_max_bytes = 65536'
                ),
            ),
        )
    )

    source = config.sites['test'].sources['admin']
    assert source.max_event_bytes + 117 == config.daemon.max_ipc_frame_bytes
    assert source.queue_max_bytes == config.daemon.max_ipc_frame_bytes
    assert source.commit_batch_max_bytes >= source.max_event_bytes + 121


def test_plugin_config_rejects_canonical_bytes_above_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    oversized = 'x' * 262_145

    with pytest.raises(ConfigError, match=r'plugin config.*262144 bytes'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, plugin_config=f'value = "{oversized}"'),
            )
        )


@pytest.mark.parametrize(
    'bind',
    [str(ipaddress.IPv4Address(0)), '127.0.0.1', '::', '::1'],
)
def test_probe_bind_accepts_ip_address_literals(
    bind: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    config = load_config(
        _write(
            tmp_path,
            _config_text(plugin_path, daemon_extra=f'probe_bind = "{bind}"'),
        )
    )

    assert config.daemon.probe_bind == bind


@pytest.mark.parametrize('bind', ['localhost', '127.0.0.999', '::gg'])
def test_probe_bind_rejects_hostnames_and_malformed_addresses(
    bind: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=r'probe_bind.*IP address literal'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, daemon_extra=f'probe_bind = "{bind}"'),
            )
        )


def test_probe_configuration_retains_strict_unknown_key_rejection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=r'daemon.*unknown keys.*probe_address'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, daemon_extra='probe_address = "127.0.0.1"'),
            )
        )


@pytest.mark.parametrize('port', [1, 65_535])
def test_probe_port_accepts_inclusive_tcp_range(
    port: int,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    config = load_config(
        _write(
            tmp_path,
            _config_text(plugin_path, daemon_extra=f'probe_port = {port}'),
        )
    )

    assert config.daemon.probe_port == port


@pytest.mark.parametrize('value', ['0', '65536', 'true', '1.0', '"8080"'])
def test_probe_port_rejects_values_outside_strict_integer_range(
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=r'probe_port.*integer.*1.*65535'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, daemon_extra=f'probe_port = {value}'),
            )
        )


@pytest.mark.parametrize(('location', 'field'), _COUNT_FIELDS)
@pytest.mark.parametrize('value', ['0', 'nan', 'inf', '-inf'])
def test_count_and_byte_limits_must_be_positive_integers(
    location: str,
    field: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=f'{field}.*integer.*at least 1'):
        load_config(
            _write(
                tmp_path,
                _numeric_config(plugin_path, location, f'{field} = {value}'),
            )
        )


@pytest.mark.parametrize(('location', 'field'), _DURATION_FIELDS)
@pytest.mark.parametrize('value', ['0', 'nan', 'inf', '-inf'])
def test_durations_must_be_positive_and_finite(
    location: str,
    field: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=f'{field}.*positive and finite'):
        load_config(
            _write(
                tmp_path,
                _numeric_config(plugin_path, location, f'{field} = {value}'),
            )
        )


@pytest.mark.parametrize(
    ('extra', 'match'),
    [
        ('unexpected = true', 'unknown keys.*unexpected'),
        ('enabled = true', 'unknown keys.*enabled'),
    ],
)
def test_group_rejects_unknown_keys(
    extra: str,
    match: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=match):
        load_config(_write(tmp_path, _config_text(plugin_path, group_extra=extra)))


@pytest.mark.parametrize(
    ('credential_line', 'plugin_secret', 'match'),
    [
        ('api_key = "plaintext"', '', 'api_key.*environment reference'),
        (
            'api_key = { env = "SG_EVENT_ADMIN_KEY" }',
            'token = "plaintext"',
            'secrets.token.*environment reference',
        ),
    ],
)
def test_inline_secrets_are_rejected(
    credential_line: str,
    plugin_secret: str,
    match: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    text = _config_text(plugin_path, plugin_secrets=plugin_secret)
    text = text.replace(
        'api_key = { env = "SG_EVENT_ADMIN_KEY" }',
        credential_line,
        1,
    )

    with pytest.raises(ConfigError, match=match):
        load_config(_write(tmp_path, text))


@pytest.mark.parametrize('case', _LIMIT_RELATIONSHIP_CASES)
def test_limit_relationships_are_rejected(
    case: tuple[str, str, str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    daemon_extra, source_extra, group_extra, match = case
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=match):
        load_config(
            _write(
                tmp_path,
                _config_text(
                    plugin_path,
                    daemon_extra=daemon_extra,
                    group_extra=group_extra,
                    source_extra=source_extra,
                ),
            )
        )


@pytest.mark.parametrize(
    ('env_name', 'plugin_secrets'),
    [
        ('SG_EVENT_ADMIN_KEY', ''),
        ('PLUGIN_TOKEN', 'token = { env = "PLUGIN_TOKEN" }'),
    ],
)
def test_missing_secret_environment_variables_are_rejected(
    env_name: str,
    plugin_secrets: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    monkeypatch.delenv(env_name, raising=False)
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=f'environment variable {env_name} is not set'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, plugin_secrets=plugin_secrets),
            )
        )


@pytest.mark.parametrize('value', _NON_JSON_VALUES)
def test_plugin_config_rejects_non_json_toml_values(
    value: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=r'plugin config.*JSON-compatible'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, plugin_config=f'value = {value}'),
            )
        )


def test_relative_state_dir_is_resolved_against_config_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    text = _config_text(_plugin_file(tmp_path))
    assert 'state_dir = "/tmp/sg-event-daemon-test"' in text
    path = _write(
        tmp_path,
        text.replace('state_dir = "/tmp/sg-event-daemon-test"', 'state_dir = "state"'),
    )
    expected = path.resolve().parent / 'state'

    assert load_config(path).daemon.state_dir == expected
    assert load_control_config(path).state_dir == expected


@pytest.mark.parametrize('worker_name', ['default', 'dedicated'])
def test_reserved_worker_names_are_rejected(
    worker_name: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    valid_config = _config_text(plugin_path)
    valid_site = load_config(_write(tmp_path, valid_config, name='valid.toml')).sites['test']
    assert 'noop' in valid_site.groups
    assert valid_site.workers == {}
    worker_table = f'[sites.test.workers.{worker_name}]\n'

    with pytest.raises(ConfigError, match=rf'^reserved worker name {worker_name}$'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, worker_table=worker_table),
            )
        )


def test_secrets_are_explicit_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plugin_path = _plugin_file(tmp_path)
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'credential-secret')
    monkeypatch.setenv('PLUGIN_TOKEN', 'plugin-secret')
    path = _write(
        tmp_path,
        _config_text(
            plugin_path,
            plugin_config='nested = { values = [1, 2, { enabled = true }] }',
            plugin_secrets='token = { env = "PLUGIN_TOKEN" }',
        ),
    )

    config = load_config(path)

    api_key = config.sites['test'].credentials['admin'].api_key
    plugin = config.sites['test'].groups['noop'].plugins[0]
    assert isinstance(api_key, SecretValue)
    assert api_key.reveal() == 'credential-secret'
    assert repr(api_key) == '<redacted>'
    assert str(api_key) == '<redacted>'
    config_objects = (
        config,
        config.daemon,
        config.sites['test'],
        config.sites['test'].credentials['admin'],
        config.sites['test'].sources['admin'],
        config.sites['test'].groups['noop'],
        plugin,
    )
    assert all('credential-secret' not in repr(value) for value in config_objects)
    assert all('credential-secret' not in str(value) for value in config_objects)
    assert plugin.secrets == {'token': SecretRef(env='PLUGIN_TOKEN')}
    assert isinstance(plugin.config, MappingProxyType)
    nested = plugin.config['nested']
    assert isinstance(nested, MappingProxyType)
    assert isinstance(nested['values'], tuple)
    set_daemon_attribute = config.daemon.__setattr__
    with pytest.raises(FrozenInstanceError):
        set_daemon_attribute('health_interval_secs', 3.0)
    with pytest.raises(TypeError):
        cast('dict[str, object]', plugin.config)['new'] = 'value'


def test_shotgun_api3_rejects_async_groups(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match=r'async.*sg-python-api'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, group_extra='execution = "async"'),
            )
        )


def test_source_names_cannot_share_an_event_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    path = _write(
        tmp_path,
        _config_text(_plugin_file(tmp_path))
        + '\n[sites.test.credentials.first]\nscript_name = "first"\n'
        + 'api_key = { env = "SG_EVENT_ADMIN_KEY" }\n'
        + '\n[sites.test.credentials.second]\nscript_name = "second"\n'
        + 'api_key = { env = "SG_EVENT_ADMIN_KEY" }\n'
        + '\n[sites.test.sources.a_b]\ncredentials = "first"\n'
        + '\n[sites.test.sources.a-b]\ncredentials = "second"\n',
    )

    with pytest.raises(ConfigError, match='event socket'):
        load_config(path)


def test_source_pending_obligations_cover_group_count(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    text = (
        _config_text(plugin_path, source_extra='max_pending_group_obligations = 1')
        + f'''
[sites.test.groups.second]

[[sites.test.groups.second.plugins]]
id = "noop"
path = "{plugin_path}"
'''
    )

    with pytest.raises(
        ConfigError,
        match=r'max_pending_group_obligations.*configured group count',
    ):
        load_config(_write(tmp_path, text))


def test_top_level_rejects_unknown_keys(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)
    text = 'unknown = true\n' + _config_text(plugin_path)

    with pytest.raises(ConfigError, match=r'unknown keys.*unknown'):
        load_config(_write(tmp_path, text))


def test_unknown_workers_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'secret')
    plugin_path = _plugin_file(tmp_path)

    with pytest.raises(ConfigError, match='unknown worker missing'):
        load_config(
            _write(
                tmp_path,
                _config_text(plugin_path, group_extra='worker = "missing"'),
            )
        )
