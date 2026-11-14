from __future__ import annotations

from pathlib import Path

import pytest

from tests._signals import publish_text, read_complete_lines


def test_append_reader_waits_for_the_record_terminator(tmp_path: Path) -> None:
    records = tmp_path / 'records'
    assert read_complete_lines(records) == []
    records.write_text('first complete\npartial')
    assert read_complete_lines(records) == ['first complete']
    with records.open('a') as stream:
        stream.write(' record\nmalformed but complete\n')
    assert read_complete_lines(records) == [
        'first complete',
        'partial record',
        'malformed but complete',
    ]


@pytest.mark.parametrize('previous', [None, 'previous value'])
def test_marker_becomes_visible_only_with_complete_content(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, previous: str | None
) -> None:
    marker = tmp_path / 'answered'
    if previous is not None:
        marker.write_text(previous)
    observed: list[str | None] = []

    def interrupted_write(path: Path, value: str, *, encoding: str) -> int:
        with path.open('w', encoding=encoding) as stream:
            observed.append(marker.read_text() if marker.exists() else None)
            stream.write(value[:3])
            stream.flush()
            observed.append(marker.read_text() if marker.exists() else None)
            return 3 + stream.write(value[3:])

    monkeypatch.setattr(Path, 'write_text', interrupted_write)
    publish_text(marker, '123.456')
    assert observed == [previous, previous]
    assert marker.read_text() == '123.456'
    assert list(tmp_path.iterdir()) == [marker]
