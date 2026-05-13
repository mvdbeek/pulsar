"""Static capability snapshot collection for a Pulsar app.

A ``PulsarCapabilities`` is a serializable summary of what a Pulsar
server is configured to do — staging paths, dependency resolvers,
container runtimes available on the host — so that Galaxy can read it
at job-build time and adjust (or refuse) requests that the remote does
not actually support.

The data is collected once at app startup (in ``PulsarApp.__init__``),
cached on the app, and published once to a relay topic from
``messaging.bind_app``. It is never recomputed. Consumers fetch the
latest snapshot via the relay's REST topic-messages endpoint.

Schema is versioned (``SCHEMA_VERSION``); bumping it is a wire-breaking
change for consumers, so add new optional fields rather than reshape
existing ones.
"""
from __future__ import annotations

import logging
import os
import shutil
from dataclasses import (
    asdict,
    dataclass,
    field,
)
from typing import (
    Any,
    Optional,
    Union,
)

from pulsar import __version__ as pulsar_version

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1


@dataclass
class DependencyResolverInfo:
    type: str
    disabled: bool = False
    versionless: bool = False
    auto_init: Optional[bool] = None
    auto_install: Optional[bool] = None
    prefix: Optional[str] = None


@dataclass
class ContainerRuntimeInfo:
    docker_available: bool = False
    singularity_available: bool = False
    apptainer_available: bool = False


@dataclass
class ManagerCapabilities:
    name: str
    type: str
    num_concurrent_jobs: Union[int, str, None] = None


@dataclass
class PulsarCapabilities:
    schema_version: int
    manager_name: str
    pulsar_version: str
    staging_directory: str
    persistence_directory: Optional[str]
    tool_dependency_dir: Optional[str]
    dependency_resolvers: list = field(default_factory=list)
    conda_available: bool = False
    container_runtime: ContainerRuntimeInfo = field(default_factory=ContainerRuntimeInfo)
    manager: Optional[ManagerCapabilities] = None

    def to_dict(self) -> dict:
        return asdict(self)


def collect_capabilities(app: Any, manager: Any) -> PulsarCapabilities:
    """Build the capabilities snapshot for a single manager on this app.

    ``app`` is a ``PulsarApp``; ``manager`` is a ``StatefulManagerProxy``
    instance (the kind held in ``app.managers``). Both are duck-typed so
    that unit tests can pass minimal stand-ins.
    """
    resolvers = _collect_dependency_resolvers(app)
    conda_available = _conda_available(resolvers)
    container_runtime = _detect_container_runtime()
    manager_caps = _collect_manager(manager)
    return PulsarCapabilities(
        schema_version=SCHEMA_VERSION,
        manager_name=manager_caps.name,
        pulsar_version=pulsar_version,
        staging_directory=getattr(app, "staging_directory", ""),
        persistence_directory=getattr(app, "persistence_directory", None),
        tool_dependency_dir=_tool_dependency_dir(app),
        dependency_resolvers=resolvers,
        conda_available=conda_available,
        container_runtime=container_runtime,
        manager=manager_caps,
    )


def _collect_dependency_resolvers(app: Any) -> list:
    dm = getattr(app, "dependency_manager", None)
    if dm is None:
        return []
    out = []
    for r in getattr(dm, "dependency_resolvers", []) or []:
        rt = getattr(r, "resolver_type", None)
        if not rt:
            continue
        info = DependencyResolverInfo(
            type=str(rt),
            disabled=bool(getattr(r, "disabled", False)),
            versionless=bool(getattr(r, "versionless", False)),
        )
        if rt == "conda":
            info.auto_init = _maybe_bool(getattr(r, "auto_init", None))
            info.auto_install = _maybe_bool(getattr(r, "auto_install", None))
            prefix = getattr(r, "prefix", None)
            info.prefix = str(prefix) if prefix else None
        out.append(info)
    return out


def _conda_available(resolvers: list) -> bool:
    """A conda resolver is enabled and conda is reachable on this host.

    "Reachable" means either the conda binary is on PATH or the resolver's
    configured prefix exists on disk — Galaxy on the requesting side wants
    to know whether ``dependency_resolution=remote`` will actually find
    something to run.
    """
    for r in resolvers:
        if r.type != "conda" or r.disabled:
            continue
        if shutil.which("conda"):
            return True
        if r.prefix and os.path.isdir(r.prefix):
            return True
    return False


def _detect_container_runtime() -> ContainerRuntimeInfo:
    return ContainerRuntimeInfo(
        docker_available=bool(shutil.which("docker")),
        singularity_available=bool(shutil.which("singularity")),
        apptainer_available=bool(shutil.which("apptainer")),
    )


def _collect_manager(manager: Any) -> ManagerCapabilities:
    name = getattr(manager, "name", "_default_")
    underlying = getattr(manager, "_proxied_manager", manager)
    mtype = getattr(underlying.__class__, "manager_type", "unknown")
    num = _num_concurrent_jobs(underlying)
    return ManagerCapabilities(name=name, type=mtype, num_concurrent_jobs=num)


def _num_concurrent_jobs(underlying: Any) -> Union[int, str, None]:
    work_threads = getattr(underlying, "work_threads", None)
    if work_threads is not None:
        try:
            return len(work_threads)
        except TypeError:
            return None
    return getattr(underlying, "num_concurrent_jobs", None)


def _tool_dependency_dir(app: Any) -> Optional[str]:
    dm = getattr(app, "dependency_manager", None)
    if dm is None:
        return None
    base = getattr(dm, "default_base_path", None)
    return str(base) if base else None


def _maybe_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    return bool(v)


def collect_all_capabilities(app: Any) -> dict:
    """Map ``manager_name -> PulsarCapabilities`` for every manager on the app.

    Convenience wrapper used by ``PulsarApp.__init__`` to populate the
    cache; failures on individual managers are logged and skipped so a
    misconfigured manager cannot prevent the app from coming up.
    """
    out: dict = {}
    for name, manager in (getattr(app, "managers", {}) or {}).items():
        try:
            out[name] = collect_capabilities(app, manager)
        except Exception:
            log.exception("Failed to collect capabilities for manager %s", name)
    return out
