from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

from sg_event_daemon._gaps import (
    _count_ids,
    _format_runs,
    clip_runs,
    id_runs,
    log_catchup_skipped,
    log_closeout_gave_up,
    log_closeout_retrying,
    log_gap_filled,
    log_ids_given_up,
    log_paused,
    log_resumed,
    subtract_ids,
)


def test_format_runs_truncates_after_ten() -> None:
    runs = tuple((n * 10, n * 10 + (n % 2)) for n in range(1, 14))
    assert _format_runs(runs[:2]) == '10-11, 20'
    assert _format_runs(runs).endswith(', 100 and 3 more')


def test_id_runs_and_arithmetic() -> None:
    assert id_runs([3, 4, 5, 9, 11, 12]) == ((3, 5), (9, 9), (11, 12))
    assert id_runs([]) == ()
    with pytest.raises(ValueError, match='ascend'):
        id_runs([3, 3])
    with pytest.raises(ValueError, match='ascend'):
        id_runs([3, 2])

    assert subtract_ids(((10, 20),), [10, 12, 13, 20]) == ((11, 11), (14, 19))
    assert subtract_ids(((10, 12), (20, 22)), [1, 11, 21, 30]) == (
        (10, 10),
        (12, 12),
        (20, 20),
        (22, 22),
    )
    assert clip_runs(((1, 5), (8, 12)), low=4, high=9) == ((4, 5), (8, 9))
    assert clip_runs(((1, 5),), low=6) == ()
    assert _count_ids(((1, 5), (8, 8))) == 6


@pytest.mark.parametrize(
    ('emit', 'args', 'expected'),
    [
        (
            log_gap_filled,
            (100, 120, 2.25),
            (
                'late events in IDs 100-120 all appeared; gap closed after 2.2s',
                'sg_event_daemon.source',
                'INFO',
            ),
        ),
        (
            log_ids_given_up,
            (((101, 102), (110, 110)), 30.05),
            (
                'gave up on 3 event IDs after 30.1s: 101-102, 110',
                'sg_event_daemon.source',
                'WARNING',
            ),
        ),
        (
            log_paused,
            ('max_open_ranges', 100, 101),
            (
                (
                    'source paused: max_open_ranges limit 100 reached (101); '
                    'open gaps continue and new gaps wait'
                ),
                'sg_event_daemon.source',
                'WARNING',
            ),
        ),
        (
            log_resumed,
            (4.25,),
            ('source resumed after 4.2s paused', 'sg_event_daemon.source', 'INFO'),
        ),
        (
            log_closeout_retrying,
            (100, 120, ((101, 102), (110, 110)), 10.25),
            (
                (
                    'SG did not return 3 events the source saw in IDs 100-120: 101-102, 110; '
                    'retrying, and skipping them if still missing after 10.2s'
                ),
                'sg_event_daemon.group',
                'WARNING',
            ),
        ),
        (
            log_closeout_gave_up,
            (100, 120, ((101, 102), (110, 110))),
            (
                (
                    'gave up on 3 events SG did not return in IDs 100-120: 101-102, 110; '
                    'replay the group from 101 to run them if they become readable'
                ),
                'sg_event_daemon.group',
                'WARNING',
            ),
        ),
        (
            log_catchup_skipped,
            (100, 120, 5.25),
            (
                (
                    'no events in IDs 100-120 on two reads 5.2s apart; skipped them; '
                    'replay the group from 100 if they appear'
                ),
                'sg_event_daemon.group',
                'WARNING',
            ),
        ),
    ],
)
def test_log_lines_use_exact_text_level_logger_and_extras(
    caplog: pytest.LogCaptureFixture,
    emit: Callable[..., None],
    args: tuple[object, ...],
    expected: tuple[str, str, str],
) -> None:
    message, logger_name, level_name = expected
    root = logging.getLogger('sg_event_daemon')
    old_level = root.level
    old_propagate = root.propagate
    root.addHandler(caplog.handler)
    root.setLevel(logging.INFO)
    root.propagate = False
    extra = {'site': 'studio', 'source': 'events'}
    if logger_name.endswith('.group'):
        extra['group'] = 'artists'
    try:
        emit(extra, *args)
    finally:
        root.removeHandler(caplog.handler)
        root.setLevel(old_level)
        root.propagate = old_propagate

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.getMessage() == message
    assert (record.name, record.levelname) == (logger_name, level_name)
    for key, value in extra.items():
        assert getattr(record, key) == value
    assert ('group' in record.__dict__) == (logger_name == 'sg_event_daemon.group')
