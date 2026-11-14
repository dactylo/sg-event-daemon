from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Protocol, cast


class EventContext(Protocol):
    delivery_attempt: int
    event: Mapping[str, object]
    is_operator_replay: bool


class _Registrar(Protocol):
    def add_callback(self, **values: object) -> None: ...

    @property
    def config(self) -> Mapping[str, object]: ...


def append(path: Path, row: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(dict(row), ensure_ascii=True, separators=(',', ':')) + '\n')
        stream.flush()


def callback(group: str, output_path: Path, project_id: int) -> Callable[[EventContext], None]:

    def callback(context: EventContext) -> None:
        event = context.event
        entity = event.get('entity')
        if not isinstance(entity, Mapping):
            return
        entity_mapping = cast('Mapping[str, object]', entity)
        project = event.get('project')
        if (
            entity_mapping.get('type') != 'Shot'
            or not isinstance(project, Mapping)
            or cast('Mapping[str, object]', project).get('id') != project_id
        ):
            return
        metadata = event.get('meta')
        metadata_mapping = (
            cast('Mapping[str, object]', metadata) if isinstance(metadata, Mapping) else None
        )
        phase = None if metadata_mapping is None else metadata_mapping.get('new_value')
        append(
            output_path,
            {
                'delivered_at': time.time(),
                'event_id': event['id'],
                'event_type': event['event_type'],
                'attribute_name': event['attribute_name'],
                'entity_type': entity_mapping['type'],
                'entity_id': entity_mapping['id'],
                'delivery_attempt': context.delivery_attempt,
                'is_operator_replay': context.is_operator_replay,
                'group': group,
                'phase_value': phase if isinstance(phase, str) else None,
            },
        )

    return callback


def register(registrar: _Registrar) -> None:
    project_id = registrar.config.get('project_id')
    output_path = registrar.config.get('output_path')
    if type(project_id) is not int or not isinstance(output_path, str):
        raise ValueError('probe config requires integer project_id and string output_path')
    registrar.add_callback(
        id='record-shot',
        callback=callback(Path(output_path).stem, Path(output_path), project_id),
        events=None,
    )
