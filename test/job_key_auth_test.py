"""Unit tests for ``pulsar.client.job_key_auth``."""

import pytest

from pulsar.client.job_key_auth import (
    auth_header_from_url,
    extract_and_strip_job_key,
)


class TestAuthHeaderFromUrl:
    def test_extracts_job_key(self):
        url = "https://galaxy.example/api/jobs/123/files?job_key=abc123&path=/etc"
        assert auth_header_from_url(url) == {"Authorization": "Bearer abc123"}

    def test_returns_empty_when_no_job_key(self):
        url = "https://galaxy.example/api/jobs/123/files?path=/etc"
        assert auth_header_from_url(url) == {}

    def test_returns_empty_when_no_query_string(self):
        assert auth_header_from_url("https://galaxy.example/api/jobs/123/files") == {}

    def test_returns_empty_for_none(self):
        assert auth_header_from_url(None) == {}

    def test_returns_empty_for_empty_string(self):
        # Empty string is falsy — short-circuit before urlparse so we don't
        # depend on whatever ``urlparse("")`` happens to do.
        assert auth_header_from_url("") == {}

    def test_first_value_wins_for_duplicate_job_key(self):
        # ``parse_qs`` returns a list when a key appears more than once.
        # Pick the first — there is no realistic deployment that supplies
        # two and we don't want a TypeError to leak out.
        url = "https://galaxy.example/api/jobs/123/files?job_key=first&job_key=second"
        assert auth_header_from_url(url) == {"Authorization": "Bearer first"}

    @pytest.mark.parametrize("malformed", ["://not a url", "http://[invalid"])
    def test_malformed_url_returns_empty(self, malformed):
        # The helper is in the request path — it must never raise. A
        # malformed URL just means "no header"; the request goes out and
        # whatever server is on the other side returns a 4xx.
        assert auth_header_from_url(malformed) == {}


class TestExtractAndStripJobKey:
    def test_strips_job_key_returns_secret_and_bare_url(self):
        secret, bare = extract_and_strip_job_key(
            "https://galaxy.example/api/jobs/123/files?job_key=abc123&path=/etc"
        )
        assert secret == "abc123"
        # ``path`` survives; ``job_key`` is gone.
        assert "job_key" not in bare
        assert "path=%2Fetc" in bare

    def test_strips_when_job_key_is_only_param(self):
        secret, bare = extract_and_strip_job_key(
            "https://galaxy.example/api/jobs/123/files?job_key=abc"
        )
        assert secret == "abc"
        assert bare == "https://galaxy.example/api/jobs/123/files"

    def test_returns_original_url_when_no_job_key(self):
        original = "https://galaxy.example/api/jobs/123/files?path=/etc"
        secret, bare = extract_and_strip_job_key(original)
        assert secret is None
        assert bare == original

    def test_handles_none(self):
        assert extract_and_strip_job_key(None) == (None, None)

    def test_handles_empty_string(self):
        assert extract_and_strip_job_key("") == (None, "")

    def test_preserves_fragment_and_other_query_params(self):
        secret, bare = extract_and_strip_job_key(
            "https://galaxy.example/path?a=1&job_key=secret&b=2#frag"
        )
        assert secret == "secret"
        # Fragment and other params round-trip intact (urlencode may
        # reorder; assert both are present).
        assert "a=1" in bare and "b=2" in bare
        assert "#frag" in bare
        assert "job_key" not in bare
