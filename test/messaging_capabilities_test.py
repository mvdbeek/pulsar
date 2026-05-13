"""Unit tests for the relay capabilities publisher.

The publisher is intentionally simple — one POST per startup, errors
swallowed — so these tests pin down the small set of guarantees that
the rest of the design relies on:

* ``message_queue_publish_capabilities=False`` skips the publish.
* A missing snapshot logs a warning rather than crashing.
* ``RelayTransportError`` from ``post_message`` is swallowed.
* The topic name reflects the manager and prefix.
* ``published_at`` is stamped at publish time, not at collection time.
"""
import importlib.util
from unittest import mock

import pytest

# ``pulsar-relay-client`` requires Python >=3.10 (PEP 508 marker on the
# requirements pin). Skip the entire module on older interpreters where
# the relay code path is unreachable but pulsar itself still installs.
pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("pulsar_relay_client") is None,
    reason="pulsar-relay-client requires Python >=3.10",
)

if importlib.util.find_spec("pulsar_relay_client") is not None:
    from pulsar_relay_client import RelayTransportError
else:
    RelayTransportError = Exception  # placeholder so the module imports under py3.7

from pulsar.capabilities import (  # noqa: E402 — guarded above
    ContainerRuntimeInfo,
    ManagerCapabilities,
    PulsarCapabilities,
)
from pulsar.messaging import bind_relay  # noqa: E402 — guarded above


def _caps(manager_name="_default_"):
    return PulsarCapabilities(
        schema_version=1,
        manager_name=manager_name,
        pulsar_version="0.0.0-test",
        staging_directory="/s",
        persistence_directory="/p",
        tool_dependency_dir=None,
        dependency_resolvers=[],
        conda_available=False,
        container_runtime=ContainerRuntimeInfo(),
        manager=ManagerCapabilities(name=manager_name, type="queued_python", num_concurrent_jobs=1),
    )


class _FakeManager:
    def __init__(self, name="_default_"):
        self.name = name


class _FakeApp:
    def __init__(self, manager_name="_default_"):
        self.capabilities_by_manager = {manager_name: _caps(manager_name)}


def test_publishes_once_with_expected_topic_and_payload():
    app = _FakeApp()
    transport = mock.Mock()
    bind_relay.publish_manager_capabilities_to_relay(app, _FakeManager(), transport, conf={})
    transport.post_message.assert_called_once()
    topic, payload = transport.post_message.call_args[0]
    assert topic == "pulsar_capabilities"
    assert payload["schema_version"] == 1
    assert payload["manager_name"] == "_default_"
    assert "published_at" in payload  # stamped at publish time


def test_topic_prefix_is_applied():
    app = _FakeApp()
    transport = mock.Mock()
    bind_relay.publish_manager_capabilities_to_relay(
        app, _FakeManager(), transport, conf={"relay_topic_prefix": "prod"},
    )
    topic = transport.post_message.call_args[0][0]
    assert topic == "prod_pulsar_capabilities"


def test_non_default_manager_suffix():
    app = _FakeApp(manager_name="cluster_a")
    transport = mock.Mock()
    bind_relay.publish_manager_capabilities_to_relay(
        app, _FakeManager("cluster_a"), transport, conf={},
    )
    topic = transport.post_message.call_args[0][0]
    assert topic == "pulsar_capabilities_cluster_a"


def test_off_switch_skips_publish():
    app = _FakeApp()
    transport = mock.Mock()
    bind_relay.publish_manager_capabilities_to_relay(
        app, _FakeManager(), transport,
        conf={"message_queue_publish_capabilities": False},
    )
    transport.post_message.assert_not_called()


def test_missing_capabilities_logs_warning_does_not_post(caplog):
    class _Empty:
        capabilities_by_manager = {}
    transport = mock.Mock()
    bind_relay.publish_manager_capabilities_to_relay(
        _Empty(), _FakeManager(), transport, conf={},
    )
    transport.post_message.assert_not_called()
    assert any("No cached capabilities" in r.message for r in caplog.records)


def test_relay_transport_error_swallowed(caplog):
    app = _FakeApp()
    transport = mock.Mock()
    transport.post_message.side_effect = RelayTransportError("network down")
    # Must not raise
    bind_relay.publish_manager_capabilities_to_relay(
        app, _FakeManager(), transport, conf={},
    )
    assert any("Failed to publish capabilities" in r.message for r in caplog.records)


def test_arbitrary_exception_swallowed(caplog):
    # post_message could raise any number of HTTP/TLS errors; the
    # publisher must not be the thing that crashes the manager.
    app = _FakeApp()
    transport = mock.Mock()
    transport.post_message.side_effect = ValueError("boom")
    bind_relay.publish_manager_capabilities_to_relay(
        app, _FakeManager(), transport, conf={},
    )
    assert any("Failed to publish capabilities" in r.message for r in caplog.records)


def test_published_at_is_iso8601_utc():
    import datetime
    app = _FakeApp()
    transport = mock.Mock()
    bind_relay.publish_manager_capabilities_to_relay(app, _FakeManager(), transport, conf={})
    payload = transport.post_message.call_args[0][1]
    # Must round-trip through fromisoformat with timezone info.
    parsed = datetime.datetime.fromisoformat(payload["published_at"])
    assert parsed.tzinfo is not None


def test_make_capabilities_topic_name_examples():
    # Module-level dunder names aren't mangled, so reach in via __dict__.
    fn = bind_relay.__dict__["__make_capabilities_topic_name"]
    assert fn("", "_default_") == "pulsar_capabilities"
    assert fn("", "cluster_a") == "pulsar_capabilities_cluster_a"
    assert fn("prod", "_default_") == "prod_pulsar_capabilities"
    assert fn("prod", "cluster_a") == "prod_pulsar_capabilities_cluster_a"
