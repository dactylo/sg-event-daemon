from __future__ import annotations

import io
import logging
import logging.handlers
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING

import pytest

import sg_event_daemon._control as control_module
import sg_event_daemon._logging as logging_module
from sg_event_daemon._config import CredentialConfig, SecretValue, SiteConfig
from sg_event_daemon._logging import (
    _DISPATCH_FIELDS,
    _discover_redaction_values,
    _redact_value,
    _TextFormatter,
    configure_logging,
    register_redaction_values,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


@dataclass
class _RuntimeSecrets:
    auth_headers: Mapping[str, str]
    nested: object


def _raise_secret_lookup_error(secret: str) -> None:
    raise LookupError(f'lookup failed with {secret}')


def _raise_secret_os_error(secret: str) -> None:
    try:
        _raise_secret_lookup_error(secret)
    except LookupError as error:
        raise OSError(f'request failed with {secret}') from error


def _raise_secret_plain_os_error(secret: str) -> None:
    raise OSError(f'request failed with {secret}')


def _raise_secret_context_error(secret: str) -> None:
    try:
        _raise_secret_lookup_error(secret)
    except LookupError:
        _raise_secret_plain_os_error(secret)


def _raise_secret_value_error(secret: str) -> None:
    raise ValueError(f'request failed with {secret}')


@pytest.fixture(autouse=True)
def _restore_daemon_logger() -> Iterator[None]:
    logger = logging.getLogger('sg_event_daemon')
    old_handlers = list(logger.handlers)
    old_level = logger.level
    old_propagate = logger.propagate
    try:
        yield
    finally:
        for handler in logger.handlers:
            handler.close()
        logger.handlers[:] = old_handlers
        logger.setLevel(old_level)
        logger.propagate = old_propagate


def _secret(*parts: str) -> str:
    return '-'.join(parts)


def _site_config() -> SiteConfig:
    credential = CredentialConfig(
        name='source',
        script_name='daemon-poller',
        api_key=SecretValue('SUPERSECRET'),
    )
    return SiteConfig(
        name='studio',
        server='https://example.test',
        default_source='events',
        credentials=MappingProxyType({'source': credential}),
        sources=MappingProxyType({}),
        groups=MappingProxyType({}),
        workers=MappingProxyType({}),
    )


def test_configure_logging_writes_readable_text_to_stdout() -> None:
    stream = io.StringIO()
    logger = configure_logging(stream=stream)

    logger.info(
        'delivered',
        extra={
            'site': 'studio',
            'source': 'admin',
            'group': 'editorial',
            'plugin': 'notify',
            'callback': 'send',
            'event_id': 123,
            'execution_mode': 'async',
            'delivery_attempt': 2,
            'is_operator_replay': True,
            'duration': 1.25,
            'outcome': 'success',
        },
    )

    output = stream.getvalue()
    assert 'INFO' in output
    assert 'delivered' in output
    for field, value in zip(
        _DISPATCH_FIELDS,
        ('studio', 'admin', 'editorial', 'notify', 'send', 123, 'async', 2, True, 1.25, 'success'),
        strict=True,
    ):
        assert f'{field}={value}' in output
    assert isinstance(logger.handlers[0].formatter, _TextFormatter)


def test_daemon_logger_owns_one_nonpropagating_hierarchy() -> None:
    stream = io.StringIO()
    daemon_logger = configure_logging(stream=stream)
    plugin_logger = logging.getLogger('sg_event_daemon.studio.editorial.notify')

    plugin_logger.info('one record')

    assert daemon_logger.propagate is False
    assert plugin_logger.handlers == []
    assert stream.getvalue().count('one record') == 1


def test_daemon_redaction_preserves_existing_value_shapes() -> None:
    secret = _secret('daemon', 'shape', 'secret')
    register_redaction_values(SecretValue(secret))

    result = _redact_value(
        {
            7: (
                [secret],
                {secret},
                frozenset({secret}),
                deque([secret]),
                secret.encode(),
                None,
                True,
                1.5,
            )
        }
    )

    assert result == {
        '7': (
            ['<redacted>'],
            ['<redacted>'],
            ['<redacted>'],
            ['<redacted>'],
            '<redacted>',
            None,
            True,
            1.5,
        )
    }


def test_discovery_collects_secrets_not_identity_fields() -> None:
    values = _discover_redaction_values([_site_config()])

    assert 'SUPERSECRET' in values
    assert 'source' not in values
    assert 'daemon-poller' not in values


def test_discovery_keeps_collecting_plain_sensitive_mappings() -> None:
    values = _discover_redaction_values([{'auth_headers': {'X-Token': 'abc123'}}])

    assert 'abc123' in values


def test_exception_formatter_preserves_cause_chain() -> None:
    stream = io.StringIO()
    secret = _secret('chained', 'secret', 'key')
    logger = configure_logging(
        redact_from=(value for value in ({'password': secret},)),
        stream=stream,
    )

    try:
        _raise_secret_os_error(secret)
    except OSError:
        logger.exception('callback failed with %s', secret)

    output = stream.getvalue()
    exception = output
    assert isinstance(exception, str)
    assert 'LookupError: lookup failed with <redacted>' in exception
    assert 'OSError: request failed with <redacted>' in exception
    assert '_raise_secret_lookup_error' in exception
    assert '_raise_secret_os_error' in exception
    assert 'The above exception was the direct cause of the following exception:' in exception
    assert 'RuntimeError' not in exception
    assert secret not in output


def test_exception_formatter_preserves_context_chain() -> None:
    stream = io.StringIO()
    secret = _secret('context', 'secret', 'key')
    logger = configure_logging(
        redact_from=({'password': secret},),
        stream=stream,
    )

    try:
        _raise_secret_context_error(secret)
    except OSError:
        logger.exception('callback failed with %s', secret)

    output = stream.getvalue()
    exception = output
    assert isinstance(exception, str)
    assert 'LookupError: lookup failed with <redacted>' in exception
    assert 'OSError: request failed with <redacted>' in exception
    assert '_raise_secret_lookup_error' in exception
    assert '_raise_secret_context_error' in exception
    assert '_raise_secret_plain_os_error' in exception
    assert 'During handling of the above exception, another exception occurred:' in exception
    assert 'RuntimeError' not in exception
    assert secret not in output


def test_exception_formatter_preserves_non_runtime_identity() -> None:
    stream = io.StringIO()
    secret = _secret('value', 'secret', 'key')
    logger = configure_logging(
        redact_from=({'password': secret},),
        stream=stream,
    )

    try:
        _raise_secret_value_error(secret)
    except ValueError:
        logger.exception('callback failed with %s', secret)

    output = stream.getvalue()
    exception = output
    assert isinstance(exception, str)
    assert 'ValueError: request failed with <redacted>' in exception
    assert '_raise_secret_value_error' in exception
    assert 'RuntimeError' not in exception
    assert secret not in output


def test_logging_and_control_share_secret_names() -> None:
    assert control_module.SENSITIVE_NAMES is logging_module.SENSITIVE_NAMES


def test_logging_has_no_rotating_file_or_smtp_handler() -> None:
    logger = configure_logging(stream=io.StringIO())

    assert not any(
        isinstance(
            handler,
            (
                logging.handlers.BaseRotatingHandler,
                logging.handlers.SMTPHandler,
            ),
        )
        for handler in logger.handlers
    )


@pytest.mark.parametrize('mapping_args', [False, True])
def test_numeric_format_specifiers_survive_redaction(
    *,
    mapping_args: bool,
) -> None:
    stream = io.StringIO()
    logger = configure_logging(stream=stream)

    if mapping_args:
        logger.info(
            'elapsed %(duration).2f seconds for %(count)d items',
            {'count': 7, 'duration': Decimal('1.5')},
        )
    else:
        logger.info('elapsed %.2f seconds for %d items', Decimal('1.5'), 7)

    assert 'elapsed 1.50 seconds for 7 items' in stream.getvalue()


def test_opaque_scalar_rendering_is_still_redacted() -> None:
    class _Token:
        def __str__(self) -> str:
            return 'token abc123'

    stream = io.StringIO()
    logger = configure_logging(
        redact_from=({'password': SecretValue('abc123')},),
        stream=stream,
    )

    logger.info('value: %s', _Token())

    output = stream.getvalue()
    assert 'value: token <redacted>' in output
    assert 'abc123' not in output


def test_plugin_logs_render_credential_names_intact() -> None:
    stream = io.StringIO()
    logger = configure_logging(stream=stream, redact_from=(_site_config(),))

    logger.info('polling source with daemon-poller; key is %s', 'SUPERSECRET')

    output = stream.getvalue()
    assert 'polling source with daemon-poller; key is <redacted>' in output
    assert 'SUPERSECRET' not in output


def test_recursive_redaction_discovers_sensitive_shared_scalar_alias() -> None:
    stream = io.StringIO()
    shared = bytearray(b'aliased-secret').decode()
    logger = configure_logging(
        redact_from=({'display_name': shared, 'password': shared},),
        stream=stream,
    )

    logger.warning('value=%s', shared)

    assert shared not in stream.getvalue()
    assert '<redacted>' in stream.getvalue()


def test_recursive_redaction_searches_public_config_and_runtime_objects() -> None:
    stream = io.StringIO()
    api_key = 'api-key-value'
    bearer = 'Bearer bearer-value'
    plugin_secret = _secret('plugin', 'secret', 'value')
    runtime = _RuntimeSecrets(
        auth_headers={'Authorization': bearer},
        nested=[{'plugin_secret': SecretValue(plugin_secret)}],
    )
    public_config = {'credentials': {'api_key': SecretValue(api_key)}}
    logger = configure_logging(
        redact_from=(public_config, runtime),
        stream=stream,
    )

    logger.warning(
        'values: %s %s %s',
        api_key,
        bearer,
        plugin_secret,
        extra={
            'request': {
                'headers': {'Authorization': bearer},
                'credential': api_key,
            }
        },
    )

    output = stream.getvalue()
    assert api_key not in output
    assert bearer not in output
    assert plugin_secret not in output
    assert output.count('<redacted>') >= 3


def test_redact_text_replaces_registered_values() -> None:
    protected_value = _secret('direct', 'registry', 'value')
    configure_logging(stream=io.StringIO())
    register_redaction_values(SecretValue(protected_value))

    result = logging_module.redact_text(f'before {protected_value} after')

    assert result == 'before <redacted> after'


def test_redact_value_preserves_shared_acyclic_children() -> None:
    secret = _secret('shared', 'acyclic', 'secret')
    register_redaction_values(SecretValue(secret))
    shared = {'token': secret}

    result = _redact_value({'a': shared, 'b': shared})

    assert result == {
        'a': {'token': '<redacted>'},
        'b': {'token': '<redacted>'},
    }


def test_redact_value_terminates_on_self_referential_mapping_cycle() -> None:
    cyclic: dict[str, object] = {}
    cyclic['self'] = cyclic

    result = _redact_value(cyclic)

    assert result == {'self': '<redacted-cycle>'}


def test_text_redaction_does_not_resort_during_render(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    short_secret = _secret('cache', 'order', 'token')
    long_secret = f'{short_secret}-suffix'
    logger = configure_logging(
        redact_from=(
            {'password': short_secret},
            {'password': long_secret},
        ),
        stream=io.StringIO(),
    )
    formatter = logger.handlers[0].formatter
    assert isinstance(formatter, _TextFormatter)
    with pytest.raises(ValueError, match=long_secret) as raised:
        _raise_secret_value_error(long_secret)
    error = raised.value
    record = logger.makeRecord(
        logger.name,
        logging.ERROR,
        __file__,
        0,
        f'callback failed with {long_secret}',
        (),
        (type(error), error, error.__traceback__),
        None,
        extra={
            'site': long_secret,
            'source': {'credential': short_secret},
            'group': [short_secret, long_secret],
            'plugin': f'notify-{long_secret}',
        },
    )
    assert record.exc_info is not None
    record.exc_text = formatter.formatException(record.exc_info)
    record.exc_info = None

    sorted_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def blocked_sorted(*args: object, **kwargs: object) -> list[object]:
        sorted_calls.append((args, kwargs))
        raise AssertionError('redaction rendering called builtins.sorted')

    with monkeypatch.context() as blocked:
        blocked.setattr('builtins.sorted', blocked_sorted)
        outputs = (formatter.format(record), formatter.format(record))

    for output in outputs:
        assert 'callback failed with <redacted>' in output
        assert 'site=<redacted>' in output
        assert "source={'credential': '<redacted>'}" in output
        assert "group=['<redacted>', '<redacted>']" in output
        assert 'plugin=notify-<redacted>' in output
        assert 'ValueError: request failed with <redacted>' in output
        assert '_raise_secret_value_error' in output
        assert long_secret not in output
        assert short_secret not in output
        assert '<redacted>-suffix' not in output
    assert sorted_calls == []

    new_secret = _secret('cache', 'order', 'fresh')
    register_redaction_values(SecretValue(new_secret))
    refreshed_record = logger.makeRecord(
        logger.name,
        logging.INFO,
        __file__,
        0,
        f'refreshed {new_secret}',
        (),
        None,
        None,
        extra={'site': new_secret},
    )
    with monkeypatch.context() as blocked:
        blocked.setattr('builtins.sorted', blocked_sorted)
        refreshed_output = formatter.format(refreshed_record)

    assert 'refreshed <redacted>' in refreshed_output
    assert 'site=<redacted>' in refreshed_output
    assert new_secret not in refreshed_output
    assert sorted_calls == []
