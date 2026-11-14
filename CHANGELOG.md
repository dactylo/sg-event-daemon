# Changelog

## 0.2.0 (2026-11-13)

### Added

- Each source accepts `late_event_window_secs`, how long it waits for a missing event ID before it
  gives up on that ID. The default is 300 seconds.
- The daemon logs problems that stop or delay delivery or skip events, such as a worker restart and
  its cause, and logs when a source or storage problem clears.

### Changed

- Strict groups hold events for 2 seconds by default instead of 300, and wait longer only behind a
  missing lower event ID.

### Fixed

- Eager groups no longer crash their worker or skip an event that SG makes visible after events
  with higher IDs, and now receive it within seconds instead of up to five minutes later.
- Bulk work on the SG site no longer halts a source or a strict group.
- A group catching up for more than about three minutes no longer has its worker restarted as
  `stale`.
- With the `sg-python-api` provider, `validate-config` and `supervise` no longer crash when the
  `sg-python-api-async` extra is not installed.
- On macOS with less than 8 GB of memory, workers no longer fail with `No buffer space available`.

## 0.1.0 (2026-10-11)

Initial release.
