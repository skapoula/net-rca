"""Tests for NfLogsAgent - test-first development.

Covers:
  - parse_timestamp
  - extract_nf_from_pod_name (pod name parsing)
  - wildcard_match (case-insensitive wildcard matching)
  - organize_and_annotate_logs (DAG phase annotation)
  - build_loki_queries (LogQL query construction for ERROR/WARN/FATAL)
  - _check_mcp_available (MCP health check)
  - logs_agent entry point (state["logs"] update)
  - Graceful degradation on each path
  - Path selection: health check determines MCP vs direct Loki
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, _patch, patch

import pytest

from triage_agent.agents.logs_agent import (
    _check_mcp_available,
    build_loki_queries,
    extract_nf_from_pod_name,
    logs_agent,
    organize_and_annotate_logs,
    wildcard_match,
)
from triage_agent.mcp.client import MCPTimeoutError
from triage_agent.state import TriageState
from triage_agent.utils import extract_log_level as _extract_log_level
from triage_agent.utils import parse_loki_response as _parse_loki_response
from triage_agent.utils import parse_timestamp

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def logs_dag() -> dict[str, Any]:
    """DAG fixture with phase_id/actors/success_log fields expected by logs_agent."""
    return {
        "all_nfs": ["AMF", "AUSF", "UDM"],
        "phases": [
            {
                "phase_id": "auth",
                "actors": ["AMF", "AUSF"],
                "success_log": "Authentication successful",
                "failure_patterns": ["*auth*fail*", "*timeout*AUSF*"],
            },
            {
                "phase_id": "registration_accept",
                "actors": ["AMF"],
                "success_log": "Registration Accept sent",
                "failure_patterns": ["*registration*reject*", "*accept*fail*"],
            },
        ],
    }


@pytest.fixture
def sample_log_entries() -> list[dict[str, Any]]:
    """Sample Loki log entries with various NFs and messages."""
    return [
        {
            "pod": "amf-deployment-abc123",
            "message": "ERROR: authentication failed for SUCI 001010123456789",
            "level": "ERROR",
            "timestamp": 1708000000,
        },
        {
            "pod": "ausf-deployment-def456",
            "message": "WARN: timeout waiting for AUSF response from UDM",
            "level": "WARN",
            "timestamp": 1708000001,
        },
        {
            "pod": "amf-deployment-abc123",
            "message": "INFO: Registration Accept sent to UE",
            "level": "INFO",
            "timestamp": 1708000005,
        },
        {
            "pod": "udm-deployment-ghi789",
            "message": "ERROR: database connection pool exhausted",
            "level": "ERROR",
            "timestamp": 1708000002,
        },
    ]


_MODULE = "triage_agent.agents.logs_agent"


def _patch_health_check(
    available: bool | None = None,
    *,
    side_effect: Exception | None = None,
) -> _patch[Any]:
    """Patch _check_mcp_available. Returns AsyncMock compatible with asyncio.run."""
    if side_effect is not None:
        return patch(
            f"{_MODULE}._check_mcp_available",
            new_callable=AsyncMock,
            side_effect=side_effect,
        )
    return patch(
        f"{_MODULE}._check_mcp_available",
        new_callable=AsyncMock,
        return_value=available,
    )


def _patch_mcp_fetch(
    *,
    return_value: list[dict[str, Any]] | None = None,
    side_effect: Exception | None = None,
) -> _patch[Any]:
    """Patch _fetch_loki_logs (MCP path)."""
    if return_value is not None:
        return patch(
            f"{_MODULE}._fetch_loki_logs",
            new_callable=AsyncMock,
            return_value=return_value,
        )
    return patch(
        f"{_MODULE}._fetch_loki_logs",
        side_effect=side_effect or Exception("MCP unavailable"),
    )


def _patch_direct_fetch(
    *,
    return_value: list[dict[str, Any]] | None = None,
    side_effect: Exception | None = None,
) -> _patch[Any]:
    """Patch _fetch_loki_logs_direct (direct Loki path)."""
    if return_value is not None:
        return patch(
            f"{_MODULE}._fetch_loki_logs_direct",
            new_callable=AsyncMock,
            return_value=return_value,
        )
    return patch(
        f"{_MODULE}._fetch_loki_logs_direct",
        side_effect=side_effect or Exception("direct unavailable"),
    )


# ===========================================================================
# wildcard_match
# ===========================================================================


class TestWildcardMatch:
    """Tests for wildcard_match utility."""

    def test_exact_match(self) -> None:
        """Should match exact text."""
        assert wildcard_match("authentication failed", "authentication failed")

    def test_wildcard_prefix(self) -> None:
        """Should match with wildcard at start."""
        assert wildcard_match("ERROR: authentication failed", "*auth*fail*")

    def test_wildcard_suffix(self) -> None:
        """Should match with wildcard at end."""
        assert wildcard_match("timeout waiting for response", "*timeout*")

    def test_case_insensitive(self) -> None:
        """Should match case-insensitively."""
        assert wildcard_match("Authentication Failed", "*auth*fail*")

    def test_case_insensitive_all_upper(self) -> None:
        """Should match when text is ALL UPPERCASE."""
        assert wildcard_match("AUTHENTICATION FAILED", "*auth*fail*")

    def test_case_insensitive_pattern_upper(self) -> None:
        """Should match when pattern has mixed case."""
        assert wildcard_match("authentication failed", "*Auth*Fail*")

    def test_no_match(self) -> None:
        """Should return False when pattern doesn't match."""
        assert not wildcard_match("registration complete", "*auth*fail*")

    def test_empty_text(self) -> None:
        """Should handle empty text."""
        assert not wildcard_match("", "*auth*")

    def test_wildcard_only(self) -> None:
        """Wildcard-only pattern should match anything."""
        assert wildcard_match("any text here", "*")

    def test_timeout_ausf_pattern(self) -> None:
        """Should match the DAG failure pattern *timeout*AUSF*."""
        assert wildcard_match(
            "WARN: timeout waiting for AUSF response", "*timeout*AUSF*"
        )

    def test_registration_reject_pattern(self) -> None:
        """Should match *registration*reject* pattern."""
        assert wildcard_match(
            "Registration Request rejected by AMF", "*registration*reject*"
        )


# ===========================================================================
# parse_timestamp
# ===========================================================================


class TestParseTimestamp:
    """Tests for parse_timestamp(). Returns Unix epoch (int or float)."""

    def test_parses_iso8601_utc(self) -> None:
        result = parse_timestamp("2026-02-15T10:00:00Z")
        assert isinstance(result, (int, float))
        assert result == pytest.approx(1771149600, abs=1)

    def test_supports_arithmetic(self) -> None:
        result = parse_timestamp("2026-02-15T10:00:00Z")
        assert (result - 300) < result

    def test_invalid_raises(self) -> None:
        with pytest.raises((ValueError, TypeError)):
            parse_timestamp("not-a-timestamp")


# ===========================================================================
# _extract_log_level (used by direct Loki path)
# ===========================================================================


class TestExtractLogLevel:
    """Tests for _extract_log_level() used by _parse_loki_response on the direct path."""

    def test_extracts_error(self) -> None:
        assert _extract_log_level("ERROR: something went wrong") == "ERROR"

    def test_extracts_warn(self) -> None:
        assert _extract_log_level("WARN: timeout approaching") == "WARN"

    def test_extracts_fatal(self) -> None:
        assert _extract_log_level("FATAL: process crashed") == "FATAL"

    def test_extracts_info(self) -> None:
        assert _extract_log_level("INFO: startup complete") == "INFO"

    def test_extracts_debug(self) -> None:
        assert _extract_log_level("DEBUG: entering function") == "DEBUG"

    def test_case_insensitive(self) -> None:
        """Should detect level regardless of message case."""
        assert _extract_log_level("error: lowercase error msg") == "ERROR"

    def test_defaults_to_info(self) -> None:
        """Messages with no recognized level should default to INFO."""
        assert _extract_log_level("some plain text with no level") == "INFO"

    def test_priority_fatal_over_error(self) -> None:
        """FATAL should be detected before ERROR when both present."""
        assert _extract_log_level("FATAL ERROR: crash") == "FATAL"

    def test_empty_message(self) -> None:
        """Empty message should default to INFO."""
        assert _extract_log_level("") == "INFO"


# ===========================================================================
# _parse_loki_response (direct Loki HTTP path)
# ===========================================================================


class TestParseLokiResponse:
    """Tests for _parse_loki_response() — Loki JSON response parser for direct HTTP path.

    This function mirrors MCPClient.query_loki response parsing so both
    MCP and direct paths produce identical output shape.
    """

    def test_parses_single_stream(self) -> None:
        data = {
            "data": {
                "result": [
                    {
                        "stream": {
                            "k8s_pod_name": "amf-deployment-abc123",
                            "k8s_namespace_name": "5g-core",
                        },
                        "values": [
                            ["1708000000000000000", "ERROR: auth failed"],
                        ],
                    }
                ]
            }
        }
        result = _parse_loki_response(data)
        assert len(result) == 1
        assert result[0]["pod"] == "amf-deployment-abc123"
        assert result[0]["message"] == "ERROR: auth failed"
        assert result[0]["timestamp"] == 1708000000
        assert result[0]["level"] == "ERROR"

    def test_parses_multiple_streams(self) -> None:
        data = {
            "data": {
                "result": [
                    {
                        "stream": {"k8s_pod_name": "amf-deployment-abc"},
                        "values": [["1708000000000000000", "ERROR: auth failed"]],
                    },
                    {
                        "stream": {"k8s_pod_name": "ausf-deployment-def"},
                        "values": [["1708000001000000000", "WARN: timeout"]],
                    },
                ]
            }
        }
        result = _parse_loki_response(data)
        assert len(result) == 2
        assert result[0]["pod"] == "amf-deployment-abc"
        assert result[1]["pod"] == "ausf-deployment-def"

    def test_parses_multiple_values_per_stream(self) -> None:
        """Multiple log lines from the same pod/stream."""
        data = {
            "data": {
                "result": [
                    {
                        "stream": {"k8s_pod_name": "amf-deployment-abc"},
                        "values": [
                            ["1708000000000000000", "ERROR: first"],
                            ["1708000001000000000", "WARN: second"],
                        ],
                    }
                ]
            }
        }
        result = _parse_loki_response(data)
        assert len(result) == 2
        assert result[0]["level"] == "ERROR"
        assert result[1]["level"] == "WARN"

    def test_empty_result(self) -> None:
        data: dict[str, Any] = {"data": {"result": []}}
        result = _parse_loki_response(data)
        assert result == []

    def test_missing_data_key(self) -> None:
        result = _parse_loki_response({})
        assert result == []

    def test_timestamp_nanoseconds_to_seconds(self) -> None:
        """Nanosecond timestamps should be converted to Unix seconds via integer division."""
        data = {
            "data": {
                "result": [
                    {
                        "stream": {"k8s_pod_name": "amf-pod"},
                        "values": [["1708000005500000000", "INFO: msg"]],
                    }
                ]
            }
        }
        result = _parse_loki_response(data)
        # 1708000005500000000 // 1_000_000_000 = 1708000005
        assert result[0]["timestamp"] == 1708000005

    def test_pod_falls_back_to_pod_label(self) -> None:
        """When k8s_pod_name is missing, should fall back to 'pod' label."""
        data = {
            "data": {
                "result": [
                    {
                        "stream": {"pod": "ausf-deployment-def"},
                        "values": [["1708000000000000000", "ERROR: fail"]],
                    }
                ]
            }
        }
        result = _parse_loki_response(data)
        assert result[0]["pod"] == "ausf-deployment-def"

    def test_pod_empty_when_no_pod_labels(self) -> None:
        """When no pod labels exist, pod should be empty string."""
        data = {
            "data": {
                "result": [
                    {
                        "stream": {"k8s_namespace_name": "5g-core"},
                        "values": [["1708000000000000000", "ERROR: orphan"]],
                    }
                ]
            }
        }
        result = _parse_loki_response(data)
        assert result[0]["pod"] == ""

    def test_preserves_all_labels(self) -> None:
        """Each entry should include the full stream labels dict."""
        labels = {"k8s_pod_name": "amf-pod", "k8s_namespace_name": "5g-core"}
        data = {
            "data": {
                "result": [
                    {
                        "stream": labels,
                        "values": [["1708000000000000000", "INFO: msg"]],
                    }
                ]
            }
        }
        result = _parse_loki_response(data)
        assert result[0]["labels"] == labels

    def test_output_shape_matches_mcp_path(self) -> None:
        """Every entry must have: timestamp, message, labels, pod, level."""
        data = {
            "data": {
                "result": [
                    {
                        "stream": {"k8s_pod_name": "amf-pod"},
                        "values": [["1708000000000000000", "ERROR: test"]],
                    }
                ]
            }
        }
        result = _parse_loki_response(data)
        entry = result[0]
        assert set(entry.keys()) == {"timestamp", "message", "labels", "pod", "level"}


# ===========================================================================
# extract_nf_from_pod_name
# ===========================================================================


class TestExtractNfFromPodName:
    """Tests for extract_nf_from_pod_name().

    Contract: pod name like 'amf-deployment-abc123' -> 'amf'
    """

    def test_standard_deployment_pod(self) -> None:
        """Standard k8s pod name: <nf>-deployment-<hash>."""
        assert extract_nf_from_pod_name("amf-deployment-abc123") == "amf"

    def test_ausf_pod(self) -> None:
        assert extract_nf_from_pod_name("ausf-deployment-def456") == "ausf"

    def test_udm_pod(self) -> None:
        assert extract_nf_from_pod_name("udm-deployment-ghi789") == "udm"

    def test_smf_pod(self) -> None:
        assert extract_nf_from_pod_name("smf-pod-xyz") == "smf"

    def test_nrf_pod(self) -> None:
        assert extract_nf_from_pod_name("nrf-statefulset-0") == "nrf"

    def test_upf_pod(self) -> None:
        assert extract_nf_from_pod_name("upf-worker-abc") == "upf"

    def test_returns_lowercase(self) -> None:
        """NF name should be lowercase regardless of pod name casing."""
        result = extract_nf_from_pod_name("AMF-deployment-abc")
        assert result == result.lower()

    def test_returns_string(self) -> None:
        result = extract_nf_from_pod_name("amf-deployment-abc")
        assert isinstance(result, str)

    def test_mongodb_pod(self) -> None:
        """Non-NF pods like mongodb should still extract the prefix."""
        assert extract_nf_from_pod_name("mongodb-replicaset-0") == "mongodb"

    def test_single_segment_pod(self) -> None:
        """Edge case: pod name with no dashes."""
        result = extract_nf_from_pod_name("amf")
        assert result == "amf"


# ===========================================================================
# organize_and_annotate_logs
# ===========================================================================


class TestOrganizeAndAnnotateLogs:
    """Tests for organize_and_annotate_logs."""

    def test_empty_logs(self, logs_dag: dict[str, Any]) -> None:
        """Should return empty dict for empty log list."""
        result = organize_and_annotate_logs([], logs_dag)
        assert result == {}

    def test_groups_by_nf(
        self,
        sample_log_entries: list[dict[str, Any]],
        logs_dag: dict[str, Any],
    ) -> None:
        """Should group logs by NF extracted from pod name."""
        result = organize_and_annotate_logs(sample_log_entries, logs_dag)
        assert "amf" in result
        assert "ausf" in result
        assert "udm" in result

    def test_amf_has_two_entries(
        self,
        sample_log_entries: list[dict[str, Any]],
        logs_dag: dict[str, Any],
    ) -> None:
        """AMF pod appears twice in sample data."""
        result = organize_and_annotate_logs(sample_log_entries, logs_dag)
        assert len(result["amf"]) == 2

    def test_annotates_matched_failure_pattern(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """Log matching a failure_pattern should get matched_phase and matched_pattern."""
        logs = [
            {
                "pod": "amf-deployment-abc",
                "message": "ERROR: authentication failed for user",
                "level": "ERROR",
                "timestamp": 1708000000,
            },
        ]
        result = organize_and_annotate_logs(logs, logs_dag)
        entry = result["amf"][0]
        assert entry["matched_phase"] == "auth"
        assert entry["matched_pattern"] == "*auth*fail*"

    def test_no_match_leaves_none(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """Log not matching any pattern should have None annotations."""
        logs = [
            {
                "pod": "udm-deployment-xyz",
                "message": "ERROR: database connection pool exhausted",
                "level": "ERROR",
                "timestamp": 1708000000,
            },
        ]
        result = organize_and_annotate_logs(logs, logs_dag)
        entry = result["udm"][0]
        assert entry["matched_phase"] is None
        assert entry["matched_pattern"] is None

    def test_preserves_log_fields(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """Each annotated entry should preserve level, message, timestamp."""
        logs = [
            {
                "pod": "amf-deployment-abc",
                "message": "WARN: something happened",
                "level": "WARN",
                "timestamp": 1708000099,
            },
        ]
        result = organize_and_annotate_logs(logs, logs_dag)
        entry = result["amf"][0]
        assert entry["level"] == "WARN"
        assert entry["message"] == "WARN: something happened"
        assert entry["timestamp"] == 1708000099

    def test_timeout_ausf_pattern_matched(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """*timeout*AUSF* pattern should match and annotate."""
        logs = [
            {
                "pod": "ausf-deployment-def",
                "message": "WARN: timeout waiting for AUSF auth response",
                "level": "WARN",
                "timestamp": 1708000001,
            },
        ]
        result = organize_and_annotate_logs(logs, logs_dag)
        entry = result["ausf"][0]
        assert entry["matched_phase"] == "auth"
        assert entry["matched_pattern"] == "*timeout*AUSF*"


# ===========================================================================
# build_loki_queries (LogQL construction)
# ===========================================================================


class TestBuildLokiQueries:
    """Tests for build_loki_queries().

    Contract:
      Input:  DAG dict with all_nfs and phases
      Output: list of LogQL query strings

    Mirrors build_nf_queries pattern in metrics_agent.
    """

    def test_returns_list_of_strings(self, logs_dag: dict[str, Any]) -> None:
        queries = build_loki_queries(logs_dag, "5g-core")
        assert isinstance(queries, list)
        assert all(isinstance(q, str) for q in queries)

    def test_base_query_per_nf_includes_error_warn_fatal(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """Each NF must have a base query filtering ERROR|WARN|FATAL."""
        queries = build_loki_queries(logs_dag, "5g-core")
        for nf in logs_dag["all_nfs"]:
            nf_lower = nf.lower()
            base_queries = [
                q for q in queries
                if nf_lower in q and "ERROR" in q and "WARN" in q and "FATAL" in q
            ]
            assert len(base_queries) >= 1, (
                f"NF {nf} missing base ERROR/WARN/FATAL query"
            )

    def test_queries_target_5g_core_namespace(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """All queries should target the 5g-core namespace."""
        queries = build_loki_queries(logs_dag, "5g-core")
        for q in queries:
            assert "5g-core" in q

    def test_queries_use_pod_regex_for_nf(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """Queries should match pods by NF name pattern."""
        queries = build_loki_queries(logs_dag, "5g-core")
        for nf in logs_dag["all_nfs"]:
            nf_lower = nf.lower()
            nf_queries = [q for q in queries if nf_lower in q]
            assert len(nf_queries) >= 1, f"No queries for NF {nf}"

    def test_phase_specific_queries_for_actors(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """NFs that are actors in a phase should get phase-specific queries.

        AMF is actor in both phases:
          auth: success_log + 2 failure_patterns = 3 queries
          registration_accept: success_log + 2 failure_patterns = 3 queries
          + 1 base query = 7 total
        """
        queries = build_loki_queries(logs_dag, "5g-core")
        amf_queries = [q for q in queries if "amf" in q]
        assert len(amf_queries) == 7, (
            f"AMF should have 7 queries (1 base + 6 phase-specific), got {len(amf_queries)}"
        )

    def test_non_actor_nf_gets_only_base_query(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """NFs not listed as actors in any phase should only get the base query."""
        queries = build_loki_queries(logs_dag, "5g-core")
        # UDM is not an actor in any phase in logs_dag
        udm_queries = [q for q in queries if "udm" in q]
        assert len(udm_queries) == 1, (
            f"UDM (non-actor) should have only base query, got {len(udm_queries)}"
        )

    def test_empty_nfs_produces_no_queries(self) -> None:
        """Empty NF list should produce zero queries."""
        dag: dict[str, Any] = {"all_nfs": [], "phases": []}
        queries = build_loki_queries(dag, "5g-core")
        assert len(queries) == 0

    def test_failure_patterns_become_queries(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """Each failure_pattern in a phase should generate a LogQL query."""
        queries = build_loki_queries(logs_dag, "5g-core")
        # Auth phase has failure_patterns: ["*auth*fail*", "*timeout*AUSF*"]
        # These should appear (regex-converted) in queries for AMF and AUSF
        auth_fail_queries = [
            q for q in queries if "auth" in q.lower() and "fail" in q.lower()
        ]
        assert len(auth_fail_queries) >= 1

    def test_success_log_becomes_query(
        self, logs_dag: dict[str, Any]
    ) -> None:
        """success_log from each phase should generate a LogQL query for its actors."""
        queries = build_loki_queries(logs_dag, "5g-core")
        success_queries = [q for q in queries if "Authentication successful" in q]
        # AMF and AUSF are both actors in auth phase
        assert len(success_queries) >= 2


# ===========================================================================
# _check_mcp_available (health check)
# ===========================================================================


class TestCheckMcpAvailable:
    """Tests for _check_mcp_available() MCP health check."""

    @pytest.mark.asyncio
    async def test_returns_true_when_loki_ready(self) -> None:
        """Should return True when MCPClient.health_check_loki() succeeds."""
        mock_client = AsyncMock()
        mock_client.health_check_loki.return_value = True
        mock_cm = AsyncMock()
        mock_cm.__aenter__.return_value = mock_client
        with patch(f"{_MODULE}.MCPClient", return_value=mock_cm):
            result = await _check_mcp_available()
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_loki_not_ready(self) -> None:
        """Should return False when Loki /ready returns non-200."""
        mock_client = AsyncMock()
        mock_client.health_check_loki.return_value = False
        mock_cm = AsyncMock()
        mock_cm.__aenter__.return_value = mock_client
        with patch(f"{_MODULE}.MCPClient", return_value=mock_cm):
            result = await _check_mcp_available()
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_connection_error(self) -> None:
        """Connection refused should return False, not raise."""
        mock_cm = AsyncMock()
        mock_cm.__aenter__.side_effect = ConnectionError("refused")
        with patch(f"{_MODULE}.MCPClient", return_value=mock_cm):
            result = await _check_mcp_available()
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_timeout(self) -> None:
        """Timeout during health check should return False."""
        mock_client = AsyncMock()
        mock_client.health_check_loki.side_effect = MCPTimeoutError("timeout")
        mock_cm = AsyncMock()
        mock_cm.__aenter__.return_value = mock_client
        with patch(f"{_MODULE}.MCPClient", return_value=mock_cm):
            result = await _check_mcp_available()
        assert result is False


# ===========================================================================
# logs_agent entry point
# ===========================================================================


class TestLogsAgentFunction:
    """Tests for logs_agent() entry point.

    Health check and fetch paths are patched to avoid real HTTP in unit tests.
    Uses direct-Loki path (health check → False) returning [] for simplicity.
    """

    def _run_logs_agent(
        self, state: TriageState, dag: dict[str, Any]
    ) -> dict[str, Any]:
        """Run logs_agent with health check → False and direct → [] (no real I/O)."""
        state["dags"] = [dag]
        with _patch_health_check(False), _patch_direct_fetch(return_value=[]):
            return logs_agent(state)

    def test_sets_logs_in_result(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """logs_agent must return a delta dict with 'logs' key."""
        result = self._run_logs_agent(sample_initial_state, logs_dag)
        assert result["logs"] is not None
        assert isinstance(result["logs"], dict)

    def test_returns_delta_dict(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """logs_agent returns only {'logs': ...}, not full state."""
        result = self._run_logs_agent(sample_initial_state, logs_dag)
        assert isinstance(result, dict)
        assert set(result.keys()) == {"logs"}

    def test_empty_dags_returns_empty_logs(
        self,
        sample_initial_state: TriageState,
    ) -> None:
        """Should return {'logs': {}} when dags is empty list."""
        state = sample_initial_state
        state["dags"] = []
        result = logs_agent(state)
        assert result == {"logs": {}}

    def test_none_dags_does_not_raise(
        self,
        sample_initial_state: TriageState,
    ) -> None:
        """Should not raise any exception when dags is None."""
        state = sample_initial_state
        state["dags"] = None
        result = logs_agent(state)
        assert isinstance(result, dict)

    def test_logs_is_dict_not_list(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """result['logs'] must be a dict keyed by NF name, not a flat list."""
        result = self._run_logs_agent(sample_initial_state, logs_dag)
        assert isinstance(result["logs"], dict)
        assert not isinstance(result["logs"], list)

    def test_mcp_path_updates_logs(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """result['logs'] should be populated correctly via MCP path."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        mcp_logs = [
            {
                "pod": "amf-deployment-abc",
                "message": "ERROR: auth failed",
                "level": "ERROR",
                "timestamp": 1708000000,
            },
        ]
        with _patch_health_check(True), _patch_mcp_fetch(return_value=mcp_logs):
            result = logs_agent(state)
        assert result["logs"] is not None
        assert isinstance(result["logs"], dict)
        assert "amf" in result["logs"]


# ===========================================================================
# Graceful degradation on each path
# ===========================================================================


class TestLogsAgentGracefulDegradation:
    """Tests for graceful degradation when the chosen path fails.

    Each path is independent — MCP failure does NOT cascade to direct.
    """

    def test_mcp_path_failure_returns_empty_logs(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """MCP available but queries fail → empty logs (no fallback to direct)."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(True), \
             _patch_mcp_fetch(side_effect=MCPTimeoutError("Loki query timed out")):
            result = logs_agent(state)
        assert result["logs"] is not None
        assert isinstance(result["logs"], dict)

    def test_direct_path_failure_returns_empty_logs(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """MCP unavailable and direct queries fail → empty logs."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(False), \
             _patch_direct_fetch(side_effect=ConnectionError("direct refused")):
            result = logs_agent(state)
        assert result["logs"] is not None
        assert isinstance(result["logs"], dict)

    def test_mcp_path_failure_does_not_raise(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Agent must not propagate MCP query exceptions to the caller."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(True), \
             _patch_mcp_fetch(side_effect=RuntimeError("unexpected")):
            result = logs_agent(state)
            assert isinstance(result, dict)

    def test_direct_path_failure_does_not_raise(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Agent must not propagate direct-Loki exceptions to the caller."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(False), \
             _patch_direct_fetch(side_effect=RuntimeError("unexpected")):
            result = logs_agent(state)
            assert isinstance(result, dict)

    def test_failure_returns_empty_logs(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Query failure should return {'logs': {}} delta dict."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(True), \
             _patch_mcp_fetch(side_effect=MCPTimeoutError("timeout")):
            result = logs_agent(state)
        assert "logs" in result
        assert isinstance(result["logs"], dict)

    def test_direct_path_loki_timeout_returns_empty_logs(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Direct path: Loki timeout should produce empty logs, not raise."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(False), \
             _patch_direct_fetch(side_effect=TimeoutError("Loki direct timed out")):
            result = logs_agent(state)
        assert result["logs"] is not None
        assert isinstance(result["logs"], dict)

    def test_direct_path_loki_timeout_does_not_raise(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Agent must not propagate direct-path TimeoutError to the caller."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(False), \
             _patch_direct_fetch(side_effect=TimeoutError("Loki timed out")):
            result = logs_agent(state)
            assert isinstance(result, dict)

    def test_mcp_path_timeout_preserves_state(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """MCP Loki timeout should set logs to empty dict without corrupting state."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(True), \
             _patch_mcp_fetch(side_effect=MCPTimeoutError("Loki query timed out")):
            result = logs_agent(state)
        assert result["logs"] is not None
        assert "logs" in result


# ===========================================================================
# Path selection: health check determines MCP vs direct Loki
# ===========================================================================


class TestLogsAgentPathSelection:
    """Tests that the upfront health check determines which path is used.

    Two completely separate paths — no fallback between them.
    """

    def test_mcp_available_uses_mcp_path(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Health check True → MCP fetch path is called."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        mcp_logs = [
            {
                "pod": "amf-deployment-abc",
                "message": "ERROR: auth failed via MCP",
                "level": "ERROR",
                "timestamp": 1708000000,
            },
        ]
        with _patch_health_check(True), \
             _patch_mcp_fetch(return_value=mcp_logs) as mock_mcp:
            result = logs_agent(state)
        mock_mcp.assert_called()
        assert "amf" in result["logs"]

    def test_mcp_available_skips_direct(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Health check True → direct path is NOT called."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(True), \
             _patch_mcp_fetch(return_value=[]), \
             _patch_direct_fetch(return_value=[]) as mock_direct:
            logs_agent(state)
        mock_direct.assert_not_called()

    def test_mcp_unavailable_uses_direct_path(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Health check False → direct Loki path is called."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        direct_logs = [
            {
                "pod": "amf-deployment-abc",
                "message": "ERROR: auth failed",
                "level": "ERROR",
                "timestamp": 1708000000,
            },
        ]
        with _patch_health_check(False), \
             _patch_direct_fetch(return_value=direct_logs) as mock_direct:
            result = logs_agent(state)
        mock_direct.assert_called()
        assert "amf" in result["logs"]

    def test_mcp_unavailable_skips_mcp_fetch(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Health check False → MCP fetch is NOT called."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(False), \
             _patch_mcp_fetch(return_value=[]) as mock_mcp, \
             _patch_direct_fetch(return_value=[]):
            logs_agent(state)
        mock_mcp.assert_not_called()

    def test_mcp_path_logs_are_annotated(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Logs from MCP path should be annotated with DAG phases."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        mcp_logs = [
            {
                "pod": "amf-deployment-abc",
                "message": "ERROR: authentication failed for SUCI",
                "level": "ERROR",
                "timestamp": 1708000000,
            },
        ]
        with _patch_health_check(True), _patch_mcp_fetch(return_value=mcp_logs):
            result = logs_agent(state)
        entry = result["logs"]["amf"][0]
        assert entry["matched_phase"] == "auth"
        assert entry["matched_pattern"] == "*auth*fail*"

    def test_direct_path_logs_are_annotated(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Logs from direct path should be annotated with DAG phases."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        direct_logs = [
            {
                "pod": "amf-deployment-abc",
                "message": "ERROR: authentication failed for SUCI",
                "level": "ERROR",
                "timestamp": 1708000000,
            },
        ]
        with _patch_health_check(False), _patch_direct_fetch(return_value=direct_logs):
            result = logs_agent(state)
        entry = result["logs"]["amf"][0]
        assert entry["matched_phase"] == "auth"
        assert entry["matched_pattern"] == "*auth*fail*"

    def test_health_check_exception_defaults_to_direct(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """If health check itself raises, default to direct Loki path."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        direct_logs = [
            {
                "pod": "ausf-deployment-def",
                "message": "WARN: timeout waiting for AUSF response",
                "level": "WARN",
                "timestamp": 1708000001,
            },
        ]
        with _patch_health_check(side_effect=RuntimeError("health check boom")), \
             _patch_direct_fetch(return_value=direct_logs):
            result = logs_agent(state)
        assert "ausf" in result["logs"]

    def test_direct_path_multiple_nfs(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Direct path should handle logs from multiple NFs."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        direct_logs = [
            {
                "pod": "amf-deployment-abc",
                "message": "ERROR: auth failed",
                "level": "ERROR",
                "timestamp": 1708000000,
            },
            {
                "pod": "ausf-deployment-def",
                "message": "WARN: timeout waiting for AUSF response",
                "level": "WARN",
                "timestamp": 1708000001,
            },
        ]
        with _patch_health_check(False), _patch_direct_fetch(return_value=direct_logs):
            result = logs_agent(state)
        assert "amf" in result["logs"]
        assert "ausf" in result["logs"]
        assert len(result["logs"]["amf"]) == 1
        assert len(result["logs"]["ausf"]) == 1

    def test_mcp_path_receives_correct_time_window(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """MCP fetch should receive start=alert_time-300, end=alert_time+60."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(True), \
             _patch_mcp_fetch(return_value=[]) as mock_fetch:
            logs_agent(state)
        _, kwargs = mock_fetch.call_args
        alert_time = int(parse_timestamp(state["alert"]["startsAt"]))
        assert kwargs["start"] == alert_time - 300
        assert kwargs["end"] == alert_time + 60

    def test_direct_path_receives_correct_time_window(
        self,
        sample_initial_state: TriageState,
        logs_dag: dict[str, Any],
    ) -> None:
        """Direct Loki fetch should receive start=alert_time-300, end=alert_time+60."""
        state = sample_initial_state
        state["dags"] = [logs_dag]
        with _patch_health_check(False), \
             _patch_direct_fetch(return_value=[]) as mock_fetch:
            logs_agent(state)
        _, kwargs = mock_fetch.call_args
        alert_time = int(parse_timestamp(state["alert"]["startsAt"]))
        assert kwargs["start"] == alert_time - 300
        assert kwargs["end"] == alert_time + 60


# ===========================================================================
# Multi-DAG and delta return tests
# ===========================================================================


class TestLogsAgentMultiDag:
    """Tests for logs_agent multi-dag phases and delta return."""

    def test_logs_agent_returns_delta_dict_not_full_state(
        self, sample_initial_state: TriageState, sample_dags: list[dict]
    ) -> None:
        """logs_agent must return only {'logs': ...}, not the full TriageState."""
        state = sample_initial_state
        state["dags"] = sample_dags
        state["nf_union"] = sample_dags[0]["all_nfs"]

        with patch("triage_agent.agents.logs_agent.asyncio.run", return_value=[]):
            result = logs_agent(state)

        assert set(result.keys()) == {"logs"}

    def test_logs_agent_builds_queries_from_all_dags_phases(
        self, sample_initial_state: TriageState
    ) -> None:
        """build_loki_queries_from_dags unions phases from all matched DAGs."""
        from triage_agent.agents.logs_agent import build_loki_queries_from_dags

        dags = [
            {
                "all_nfs": ["AMF"],
                "phases": [{"actors": ["AMF"], "success_log": "ok", "failure_patterns": ["*fail*"]}],
            },
            {
                "all_nfs": ["SMF"],
                "phases": [{"actors": ["SMF"], "success_log": "done", "failure_patterns": ["*error*"]}],
            },
        ]
        queries = build_loki_queries_from_dags(dags, "5g-core")

        all_queries = " ".join(queries)
        assert "amf" in all_queries
        assert "smf" in all_queries

    def test_logs_agent_empty_dags_returns_empty_logs(
        self, sample_initial_state: TriageState
    ) -> None:
        """logs_agent with empty dags returns {'logs': {}}."""
        state = sample_initial_state
        state["dags"] = []

        result = logs_agent(state)

        assert result == {"logs": {}}


class TestCompressNfLogsTruncation:
    def test_long_messages_are_truncated(self) -> None:
        """Messages longer than rca_log_max_message_chars are truncated."""
        from unittest.mock import patch
        from triage_agent.agents.logs_agent import compress_nf_logs
        from triage_agent.config import TriageAgentConfig

        long_msg = "E " + "x" * 500
        logs = {"AMF": [{"level": "ERROR", "message": long_msg, "matched_phase": None, "matched_pattern": None, "timestamp": 0}]}
        with patch("triage_agent.agents.logs_agent.get_config") as mock_cfg:
            mock_cfg.return_value = TriageAgentConfig(rca_log_max_message_chars=50, rca_token_budget_logs=10_000)
            result = compress_nf_logs(logs, ["AMF"], 10_000)

        assert len(result["AMF"][0]["message"]) <= 51  # 50 chars + ellipsis
        assert result["AMF"][0]["message"].endswith("…")

    def test_short_messages_are_not_truncated(self) -> None:
        from unittest.mock import patch
        from triage_agent.agents.logs_agent import compress_nf_logs
        from triage_agent.config import TriageAgentConfig

        logs = {"AMF": [{"level": "ERROR", "message": "short", "matched_phase": None, "matched_pattern": None, "timestamp": 0}]}
        with patch("triage_agent.agents.logs_agent.get_config") as mock_cfg:
            mock_cfg.return_value = TriageAgentConfig(rca_log_max_message_chars=200, rca_token_budget_logs=10_000)
            result = compress_nf_logs(logs, ["AMF"], 10_000)

        assert result["AMF"][0]["message"] == "short"


# ===========================================================================
# NEW: Tests for BSF noise filtering (Task 5)
# ===========================================================================


class TestIsQualifyingWithNoiseFilter:
    """Tests for _is_qualifying_with_noise_filter module-level function."""

    def test_rejects_bsf_noise_even_at_error_level(self) -> None:
        """_is_qualifying_with_noise_filter returns False for BSF noise at ERROR level."""
        from triage_agent.agents.logs_agent import _is_qualifying_with_noise_filter

        entry = {
            "level": "ERROR",
            "message": "BSF selection failed: no BSF instances found",
            "matched_phase": None,
            "matched_pattern": None,
        }
        assert not _is_qualifying_with_noise_filter(entry, ["*BSF selection failed*"])

    def test_passes_real_error_through(self) -> None:
        """Real AUTH failure errors are not suppressed."""
        from triage_agent.agents.logs_agent import _is_qualifying_with_noise_filter

        entry = {
            "level": "ERROR",
            "message": "Authentication failed for SUPI imsi-208930000000001",
            "matched_phase": None,
            "matched_pattern": None,
        }
        assert _is_qualifying_with_noise_filter(entry, ["*BSF selection failed*"])

    def test_passes_dag_matched_entry_through(self) -> None:
        """Entry matched against a DAG phase is always kept even if level is INFO."""
        from triage_agent.agents.logs_agent import _is_qualifying_with_noise_filter

        entry = {
            "level": "INFO",
            "message": "Registration reject: PLMN not allowed",
            "matched_phase": "registration",
            "matched_pattern": "*reject*",
        }
        assert _is_qualifying_with_noise_filter(entry, ["*BSF*"])

    def test_rejects_noise_case_insensitively(self) -> None:
        """Pattern matching is case-insensitive."""
        from triage_agent.agents.logs_agent import _is_qualifying_with_noise_filter

        entry = {
            "level": "ERROR",
            "message": "bsf selection failed",
            "matched_phase": None,
            "matched_pattern": None,
        }
        assert not _is_qualifying_with_noise_filter(entry, ["*BSF selection failed*"])

    def test_no_noise_patterns_passes_all(self) -> None:
        """Empty noise list means nothing is suppressed."""
        from triage_agent.agents.logs_agent import _is_qualifying_with_noise_filter

        entry = {
            "level": "ERROR",
            "message": "BSF selection failed: no BSF instances found",
            "matched_phase": None,
            "matched_pattern": None,
        }
        assert _is_qualifying_with_noise_filter(entry, [])


class TestCompressNfLogsNoiseFilter:
    """Tests for BSF noise filtering integration in compress_nf_logs."""

    def test_dag_nf_bsf_noise_stripped(self) -> None:
        """BSF noise entries are stripped from DAG NFs too."""
        from unittest.mock import patch
        from triage_agent.agents.logs_agent import compress_nf_logs
        from triage_agent.config import TriageAgentConfig

        logs = {
            "smf": [
                {
                    "level": "ERROR",
                    "message": "BSF selection failed: no BSF instances found",
                    "matched_phase": None,
                    "matched_pattern": None,
                    "timestamp": "t1",
                },
                {
                    "level": "ERROR",
                    "message": "PDU session rejected: invalid APN",
                    "matched_phase": "pdu_setup",
                    "matched_pattern": None,
                    "timestamp": "t2",
                },
            ]
        }
        with patch("triage_agent.agents.logs_agent.get_config") as mock_cfg:
            mock_cfg.return_value = TriageAgentConfig(
                rca_log_max_message_chars=500,
                rca_token_budget_logs=10_000,
                log_noise_patterns=["*BSF selection failed*"],
            )
            result = compress_nf_logs(logs, nf_union=["SMF"], token_budget=10_000)

        smf_messages = [e["message"] for e in result.get("smf", [])]
        assert all("BSF" not in m for m in smf_messages), "BSF noise must be stripped from DAG NF"
        assert any("PDU session rejected" in m for m in smf_messages)

    def test_non_dag_nf_bsf_noise_stripped(self) -> None:
        """BSF noise entries are stripped from non-DAG NFs."""
        from unittest.mock import patch
        from triage_agent.agents.logs_agent import compress_nf_logs
        from triage_agent.config import TriageAgentConfig

        logs = {
            "pcf": [
                {
                    "level": "ERROR",
                    "message": "BSF query error: timeout",
                    "matched_phase": None,
                    "matched_pattern": None,
                    "timestamp": "t1",
                },
                {
                    "level": "ERROR",
                    "message": "Policy update failed",
                    "matched_phase": None,
                    "matched_pattern": None,
                    "timestamp": "t2",
                },
            ]
        }
        with patch("triage_agent.agents.logs_agent.get_config") as mock_cfg:
            mock_cfg.return_value = TriageAgentConfig(
                rca_log_max_message_chars=500,
                rca_token_budget_logs=10_000,
                log_noise_patterns=["*BSF query error*"],
            )
            result = compress_nf_logs(logs, nf_union=[], token_budget=10_000)

        pcf_messages = [e["message"] for e in result.get("pcf", [])]
        assert all("BSF" not in m for m in pcf_messages)
        assert any("Policy update failed" in m for m in pcf_messages)

    def test_empty_noise_patterns_preserves_all_errors(self) -> None:
        """With empty log_noise_patterns, no entries are suppressed."""
        from unittest.mock import patch
        from triage_agent.agents.logs_agent import compress_nf_logs
        from triage_agent.config import TriageAgentConfig

        logs = {
            "smf": [
                {
                    "level": "ERROR",
                    "message": "BSF selection failed: no BSF instances found",
                    "matched_phase": None,
                    "matched_pattern": None,
                    "timestamp": "t1",
                },
            ]
        }
        with patch("triage_agent.agents.logs_agent.get_config") as mock_cfg:
            mock_cfg.return_value = TriageAgentConfig(
                rca_log_max_message_chars=500,
                rca_token_budget_logs=10_000,
                log_noise_patterns=[],
            )
            result = compress_nf_logs(logs, nf_union=["SMF"], token_budget=10_000)

        smf_messages = [e["message"] for e in result.get("smf", [])]
        assert any("BSF" in m for m in smf_messages)
