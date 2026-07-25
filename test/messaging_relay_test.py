from pulsar.messaging.bind_relay import (
    DEFAULT_RELAY_LONG_POLL_TIMEOUT,
    _relay_long_poll_timeout,
    _status_message_metadata,
    start_consumer,
)
from pulsar.messaging.relay_state import RelayState


class RecordingLongPollTransport:
    def __init__(self, relay_state):
        self.relay_state = relay_state
        self.calls = []

    def long_poll(self, topics, timeout):
        self.calls.append((topics, timeout))
        self.relay_state.active = False
        return []


def test_relay_long_poll_timeout_defaults_to_30_seconds():
    assert _relay_long_poll_timeout({}) == DEFAULT_RELAY_LONG_POLL_TIMEOUT


def test_relay_long_poll_timeout_accepts_configured_value():
    assert _relay_long_poll_timeout({"relay_long_poll_timeout": "2"}) == 2.0


def test_start_consumer_uses_configured_long_poll_timeout():
    relay_state = RelayState()
    transport = RecordingLongPollTransport(relay_state)

    thread = start_consumer(
        transport,
        relay_state,
        ["job_setup"],
        {},
        long_poll_timeout=1.5,
    )
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert transport.calls == [(["job_setup"], 1.5)]


def test_status_message_metadata_orders_all_and_deduplicates_terminal_updates():
    assert _status_message_metadata({"job_id": "42", "status": "running"}) == {
        "ordering_key": "42"
    }
    assert _status_message_metadata({"job_id": "42", "status": "complete"}) == {
        "ordering_key": "42",
        "deduplication_key": "job-terminal:42",
    }
