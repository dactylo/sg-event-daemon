from sg_event_daemon._strict import StrictBuffer, _StrictObservation, _StrictReleaseWatermark


def strict_observations(buffer: StrictBuffer) -> tuple[_StrictObservation, ...]:
    return tuple(buffer._observation_buffer[key] for key in buffer._observation_ids())


def strict_watermark(buffer: StrictBuffer, group: str) -> _StrictReleaseWatermark:
    state = buffer._groups.get(group)
    if state is not None:
        return buffer._project_watermark(state)
    return buffer._archived_watermarks[group]
