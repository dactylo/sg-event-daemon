from sg_event_daemon._closeout import CloseoutRetries


def test_failed_pass_waits_behind_due_gaps() -> None:
    retries = CloseoutRetries(fetch_interval_secs=1, window_secs=300)
    keys = [(1, 'low'), (1, 'high')]
    for key in keys:
        retries.missed(key, 0)
    retries.take_turn(1)
    retries.failed(keys[0], 1)
    assert retries.next_retry(keys, 2) == keys[1]
    assert retries.grace_ended(keys[0], 300)


def test_failure_before_first_miss_schedules_without_grace() -> None:
    retries = CloseoutRetries(fetch_interval_secs=1, window_secs=300)
    key = (1, 'gap')
    retries.failed(key, 10)
    assert retries.retrying(key)
    assert not retries.grace_ended(key, 10000)
    assert retries.next_retry([key], 10) is None
    assert retries.next_retry([key], 11) == key
    assert retries.missed(key, 500)
    assert not retries.grace_ended(key, 799)
    assert retries.grace_ended(key, 800)


def test_keep_drops_obligations_no_longer_pending() -> None:
    retries = CloseoutRetries(fetch_interval_secs=1, window_secs=300)
    old, kept, new = (1, 'a'), (1, 'b'), (2, 'a')
    retries.missed(old, 0)
    retries.failed(kept, 0)
    retries.keep([kept, new])
    assert not retries.retrying(old)
    assert retries.retrying(kept)
    assert not retries.retrying(new)


def test_one_retry_pass_per_interval_goes_to_longest_due() -> None:
    retries = CloseoutRetries(fetch_interval_secs=1, window_secs=300)
    keys = [(1, 'z-low'), (1, 'a-high')]
    for key in keys:
        retries.missed(key, 0)
    assert retries.next_retry(keys, 1) == keys[0]
    retries.take_turn(1)
    retries.missed(keys[0], 1)
    assert retries.next_retry(keys, 1.9) is None
    assert retries.next_retry(keys, 2) == keys[1]


def test_retry_schedule_backs_off_and_ends_at_grace() -> None:
    retries = CloseoutRetries(fetch_interval_secs=1, window_secs=300)
    key = (1, 'gap')
    assert retries.missed(key, 0)
    for due in (1, 3, 7, 15, 31, 63, 127, 255, 300):
        assert retries.next_retry([key], due - 0.01) is None
        assert retries.next_retry([key], due) == key
        retries.take_turn(due)
        assert retries.grace_ended(key, due) == (due == 300)
        assert not retries.missed(key, due)
