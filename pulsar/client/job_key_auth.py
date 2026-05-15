"""Helpers for sending the per-job credential via ``Authorization: Bearer …``.

Galaxy historically embeds the per-job credential as a ``?job_key=<value>``
query parameter in the callback URLs it hands to Pulsar (``files_endpoint``,
``token_endpoint`` and the action-mapper-injected URLs derived from them).
Newer Galaxy versions also accept the credential via an
``Authorization: Bearer <value>`` request header — preferred because it
does not leak into HTTP proxy access logs, browser history, or whatever
else captures URLs.

This module extracts the credential from a Galaxy-supplied URL and produces
the header form. Pulsar's HTTP callbacks then send **both** the URL (with
the query parameter intact) and the header, so:

* Older Galaxy deployments that do not yet read the header keep working
  against the query parameter (zero coordinated cutover required).
* Newer Galaxy deployments prefer the header — see Galaxy's
  ``galaxy.job_execution.job_security.resolve_job_key``.

A future Pulsar release can drop the URL form once enough Galaxy versions
have shipped header support, at which point this module can also stop
parsing the URL and instead carry the credential separately end-to-end.
"""

from typing import (
    Dict,
    Optional,
    Tuple,
)
from urllib.parse import (
    parse_qs,
    urlencode,
    urlparse,
    urlunparse,
)

JOB_KEY_QUERY_PARAM = "job_key"
AUTHORIZATION_HEADER = "Authorization"
BEARER_PREFIX = "Bearer "


def extract_and_strip_job_key(url: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Pull ``?job_key=<value>`` out of a URL.

    Returns ``(secret, bare_url)``. ``secret`` is ``None`` when the URL has
    no ``job_key`` parameter (or no URL was supplied at all); ``bare_url``
    is the URL with the parameter removed (or the original URL if no
    parameter was present). Other query-string parameters are preserved.

    Used by ``FileActionMapper`` when ``use_bearer_auth`` is enabled — we
    parse the secret out of the incoming ``files_endpoint`` at submit
    time, persist it as a separate ``auth_secret`` field in
    ``launch_config``, and rebuild action URLs without the embedded
    credential.
    """
    if not url:
        return None, url
    try:
        parsed = urlparse(url)
    except Exception:
        return None, url
    # ``keep_blank_values`` so unrelated parameters like ``?foo=`` round-trip
    # unchanged. Drop only ``job_key``.
    query = parse_qs(parsed.query, keep_blank_values=True)
    values = query.pop(JOB_KEY_QUERY_PARAM, None)
    if not values:
        return None, url
    new_query = urlencode(query, doseq=True)
    bare = urlunparse(parsed._replace(query=new_query))
    return values[0], bare


def auth_header_from_url(url: Optional[str]) -> Dict[str, str]:
    """Return ``{"Authorization": "Bearer <job_key>"}`` for a Galaxy
    callback URL that carries the credential as ``?job_key=…``.

    Returns an empty dict (not ``None``) when no credential is present so
    callers can splat the return value into a request's ``headers`` kwarg
    without a conditional. Missing url, missing parameter, or a malformed
    query string all map to the empty case — the caller will still issue
    the request and let the server fall back to URL-based auth (or fail
    with a clear 401/403).
    """
    if not url:
        return {}
    try:
        query = parse_qs(urlparse(url).query)
    except Exception:  # malformed URL — never raise from the helper
        return {}
    values = query.get(JOB_KEY_QUERY_PARAM, [])
    if not values:
        return {}
    return {AUTHORIZATION_HEADER: f"{BEARER_PREFIX}{values[0]}"}
