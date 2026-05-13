"""Unit tests for the capabilities collector.

These tests use minimal duck-typed stand-ins for ``PulsarApp`` and the
manager proxy so we can exercise every detection branch without booting
a real app.
"""
from unittest import mock

import pytest

from pulsar import capabilities
from pulsar.capabilities import (
    SCHEMA_VERSION,
    ContainerRuntimeInfo,
    DependencyResolverInfo,
    PulsarCapabilities,
    _collect_dependency_resolvers,
    _conda_available,
    _detect_container_runtime,
    _num_concurrent_jobs,
    collect_all_capabilities,
    collect_capabilities,
)


class _Resolver:
    def __init__(self, resolver_type, **attrs):
        self.resolver_type = resolver_type
        for k, v in attrs.items():
            setattr(self, k, v)


class _DepMgr:
    def __init__(self, resolvers, base_path=None):
        self.dependency_resolvers = resolvers
        self.default_base_path = base_path


class _Manager:
    """Mimics ``StatefulManagerProxy``: name + ``_proxied_manager``."""

    def __init__(self, name, manager_type, work_threads=None):
        self.name = name
        self._proxied_manager = self._make_underlying(manager_type, work_threads)

    @staticmethod
    def _make_underlying(manager_type, work_threads):
        cls = type(
            "FakeManagerImpl_%s" % manager_type,
            (object,),
            {"manager_type": manager_type},
        )
        inst = cls()
        if work_threads is not None:
            inst.work_threads = work_threads
        return inst


class _App:
    def __init__(self, *, staging="/s", persistence="/p",
                 dep_mgr=None, managers=None):
        self.staging_directory = staging
        self.persistence_directory = persistence
        self.dependency_manager = dep_mgr
        self.managers = managers or {}


def test_schema_version_is_one():
    # If we ever bump this, every consumer needs to learn the new shape;
    # guard against accidental edits.
    assert SCHEMA_VERSION == 1


def test_minimal_app_collects_clean_payload():
    mgr = _Manager("_default_", "queued_python", work_threads=[None] * 4)
    app = _App(managers={"_default_": mgr})
    with mock.patch.object(capabilities.shutil, "which", return_value=None):
        caps = collect_capabilities(app, mgr)
    assert caps.schema_version == 1
    assert caps.manager_name == "_default_"
    assert caps.staging_directory == "/s"
    assert caps.persistence_directory == "/p"
    assert caps.dependency_resolvers == []
    assert caps.conda_available is False
    assert caps.container_runtime == ContainerRuntimeInfo()
    assert caps.manager.type == "queued_python"
    assert caps.manager.num_concurrent_jobs == 4


def test_to_dict_round_trips_through_json():
    import json
    mgr = _Manager("_default_", "queued_python", work_threads=[None])
    app = _App(managers={"_default_": mgr})
    with mock.patch.object(capabilities.shutil, "which", return_value=None):
        caps = collect_capabilities(app, mgr)
    payload = caps.to_dict()
    encoded = json.dumps(payload)
    decoded = json.loads(encoded)
    assert decoded["manager"]["type"] == "queued_python"
    assert decoded["schema_version"] == 1


def test_conda_resolver_present_and_conda_on_path():
    resolvers = [
        DependencyResolverInfo(type="conda", disabled=False, prefix="/srv/_conda"),
    ]
    with mock.patch.object(capabilities.shutil, "which", return_value="/usr/bin/conda"):
        assert _conda_available(resolvers) is True


def test_conda_resolver_present_but_not_on_path_or_disk():
    resolvers = [
        DependencyResolverInfo(type="conda", disabled=False, prefix="/nonexistent/_conda"),
    ]
    with mock.patch.object(capabilities.shutil, "which", return_value=None):
        with mock.patch.object(capabilities.os.path, "isdir", return_value=False):
            assert _conda_available(resolvers) is False


def test_conda_resolver_disabled_means_unavailable_even_if_conda_on_path():
    resolvers = [
        DependencyResolverInfo(type="conda", disabled=True, prefix="/srv/_conda"),
    ]
    with mock.patch.object(capabilities.shutil, "which", return_value="/usr/bin/conda"):
        assert _conda_available(resolvers) is False


def test_conda_available_via_existing_prefix_on_disk(tmp_path):
    resolvers = [
        DependencyResolverInfo(type="conda", disabled=False, prefix=str(tmp_path)),
    ]
    with mock.patch.object(capabilities.shutil, "which", return_value=None):
        assert _conda_available(resolvers) is True


@pytest.mark.parametrize("docker,singularity,apptainer", [
    (True, False, False),
    (False, True, False),
    (False, False, True),
    (True, True, True),
    (False, False, False),
])
def test_detect_container_runtime_cross_product(docker, singularity, apptainer):
    def fake_which(binary):
        return {
            "docker": "/usr/bin/docker" if docker else None,
            "singularity": "/usr/bin/singularity" if singularity else None,
            "apptainer": "/usr/bin/apptainer" if apptainer else None,
        }.get(binary)
    with mock.patch.object(capabilities.shutil, "which", side_effect=fake_which):
        cr = _detect_container_runtime()
    assert cr.docker_available is docker
    assert cr.singularity_available is singularity
    assert cr.apptainer_available is apptainer


def test_collects_dependency_resolver_attributes():
    resolvers = [
        _Resolver("tool_shed_packages", versionless=False, disabled=False),
        _Resolver("conda", disabled=True, auto_init=True, auto_install=False,
                  prefix="/p", versionless=True),
    ]
    app = _App(dep_mgr=_DepMgr(resolvers))
    out = _collect_dependency_resolvers(app)
    assert len(out) == 2
    assert out[0].type == "tool_shed_packages"
    assert out[0].disabled is False
    assert out[1].type == "conda"
    assert out[1].disabled is True
    assert out[1].auto_init is True
    assert out[1].auto_install is False
    assert out[1].prefix == "/p"
    assert out[1].versionless is True


def test_resolver_without_resolver_type_is_skipped():
    # Some resolver subclasses might not set resolver_type; we drop them
    # rather than emit a noisy unknown entry.
    resolvers = [_Resolver(None), _Resolver("conda", disabled=False)]
    app = _App(dep_mgr=_DepMgr(resolvers))
    out = _collect_dependency_resolvers(app)
    assert [r.type for r in out] == ["conda"]


def test_num_concurrent_jobs_prefers_work_threads_len():
    mgr = _Manager("m", "queued_python", work_threads=[None, None, None])
    assert _num_concurrent_jobs(mgr._proxied_manager) == 3


def test_num_concurrent_jobs_falls_back_to_attr():
    mgr = _Manager("m", "unqueued")
    mgr._proxied_manager.num_concurrent_jobs = 7
    assert _num_concurrent_jobs(mgr._proxied_manager) == 7


def test_num_concurrent_jobs_none_when_neither():
    mgr = _Manager("m", "unqueued")
    assert _num_concurrent_jobs(mgr._proxied_manager) is None


def test_stateful_manager_proxy_unwrap_for_manager_type():
    # The proxy doesn't expose manager_type itself; collect_capabilities
    # must reach through ``_proxied_manager`` to find it.
    mgr = _Manager("m", "queued_drmaa", work_threads=[None] * 2)
    assert getattr(mgr, "manager_type", None) is None  # confirm proxy hides it
    app = _App(managers={"m": mgr})
    with mock.patch.object(capabilities.shutil, "which", return_value=None):
        caps = collect_capabilities(app, mgr)
    assert caps.manager.type == "queued_drmaa"


def test_collect_all_capabilities_returns_per_manager_dict():
    mgr_a = _Manager("a", "queued_python", work_threads=[None])
    mgr_b = _Manager("b", "queued_drmaa", work_threads=[None] * 2)
    app = _App(managers={"a": mgr_a, "b": mgr_b})
    with mock.patch.object(capabilities.shutil, "which", return_value=None):
        out = collect_all_capabilities(app)
    assert set(out.keys()) == {"a", "b"}
    assert isinstance(out["a"], PulsarCapabilities)
    assert out["a"].manager.type == "queued_python"
    assert out["b"].manager.type == "queued_drmaa"


def test_collect_all_capabilities_skips_failing_manager(caplog):
    mgr_ok = _Manager("ok", "queued_python", work_threads=[None])

    class BoomManager:
        # Hitting `name` raises — the collector should log+skip, not crash.
        @property
        def name(self):
            raise RuntimeError("boom")
    app = _App(managers={"ok": mgr_ok, "boom": BoomManager()})
    with mock.patch.object(capabilities.shutil, "which", return_value=None):
        out = collect_all_capabilities(app)
    assert "ok" in out
    assert "boom" not in out
    assert any("Failed to collect capabilities for manager boom" in r.message
               for r in caplog.records)
