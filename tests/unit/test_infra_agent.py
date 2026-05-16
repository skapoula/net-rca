"""Tests for InfraAgent - write tests first, then implement.

This test file follows the test-first development approach.
Tests are written based on the specification before implementation.
"""

from typing import Any

import pytest

from triage_agent.agents.infra_agent import (
    build_infra_queries,
    compute_infrastructure_score,
    count_concurrent_failures,
    extract_critical_events,
    extract_nfs_from_alert,
    extract_node_status,
    extract_oom_events,
    extract_resource_metrics,
    extract_restart_counts,
    infra_agent,
)
from triage_agent.state import TriageState
from triage_agent.utils import parse_timestamp

# ---------------------------------------------------------------------------
# Fixtures (local to this module)
# ---------------------------------------------------------------------------

@pytest.fixture
def healthy_metrics() -> dict[str, Any]:
    """Metrics representing a fully healthy infrastructure."""
    return {
        "pod_restarts": [
            {"pod": "amf-deployment-abc", "container": "amf", "value": 0},
            {"pod": "ausf-deployment-def", "container": "ausf", "value": 0},
        ],
        "oom_kills": [],
        "cpu_usage": [
            {"pod": "amf-deployment-abc", "container": "amf", "value": 0.3},
            {"pod": "ausf-deployment-def", "container": "ausf", "value": 0.2},
        ],
        "memory_percent": [
            {"pod": "amf-deployment-abc", "container": "amf", "value": 45},
            {"pod": "ausf-deployment-def", "container": "ausf", "value": 38},
        ],
        "pod_status": [
            {"pod": "amf-deployment-abc", "phase": "Running"},
            {"pod": "ausf-deployment-def", "phase": "Running"},
        ],
    }


@pytest.fixture
def degraded_metrics() -> dict[str, Any]:
    """Metrics with multiple infrastructure issues."""
    return {
        "pod_restarts": [
            {"pod": "amf-deployment-abc", "container": "amf", "value": 4},
            {"pod": "ausf-deployment-def", "container": "ausf", "value": 0},
            {"pod": "udm-deployment-ghi", "container": "udm", "value": 7},
        ],
        "oom_kills": [
            {"pod": "udm-deployment-ghi", "container": "udm", "value": 2},
        ],
        "cpu_usage": [
            {"pod": "amf-deployment-abc", "container": "amf", "value": 1.5},
            {"pod": "ausf-deployment-def", "container": "ausf", "value": 0.2},
            {"pod": "udm-deployment-ghi", "container": "udm", "value": 0.8},
        ],
        "memory_percent": [
            {"pod": "amf-deployment-abc", "container": "amf", "value": 92},
            {"pod": "ausf-deployment-def", "container": "ausf", "value": 38},
            {"pod": "udm-deployment-ghi", "container": "udm", "value": 95},
        ],
        "pod_status": [
            {"pod": "amf-deployment-abc", "phase": "Running"},
            {"pod": "ausf-deployment-def", "phase": "Running"},
            {"pod": "udm-deployment-ghi", "phase": "CrashLoopBackOff"},
        ],
    }


# ===========================================================================
# Existing tests (preserved as-is)
# ===========================================================================


class TestComputeInfrastructureScore:
    """Tests for infrastructure score computation.

    Weight table:
    | Factor | Weight | Scoring |
    | Restarts | 0.35 | 0:0.0, 1-2:0.4, 3-5:0.7, >5:1.0 |
    | OOM | 0.25 | 0:0.0, >0:1.0 |
    | Pod Status | 0.20 | Running:0.0, Pending:0.6, Failed:1.0 |
    | Resources | 0.20 | Mem>90%:1.0, CPU>1.0:0.8, Normal:0.0 |
    """

    def test_all_healthy_metrics_returns_near_zero(self) -> None:
        """All healthy metrics should result in score near 0.0."""
        metrics = {
            "pod_restarts": [{"pod": "amf-1", "value": 0}],
            "oom_kills": [],
            "cpu_usage": [{"pod": "amf-1", "value": 0.3}],
            "memory_percent": [{"pod": "amf-1", "value": 50}],
            "pod_status": [{"pod": "amf-1", "phase": "Running"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.0, abs=0.01)

    def test_pod_restarts_weight(self) -> None:
        """Pod restarts should contribute 0.35 weight to score."""
        # 1-2 restarts = 0.4 factor, weight 0.35 -> 0.14 contribution
        metrics = {
            "pod_restarts": [{"pod": "amf-1", "value": 2}],
            "oom_kills": [],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [{"phase": "Running"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.35 * 0.4, abs=0.01)  # 0.14

    def test_pod_restarts_high_count(self) -> None:
        """More than 5 restarts should max out restart factor."""
        metrics = {
            "pod_restarts": [{"pod": "amf-1", "value": 10}],
            "oom_kills": [],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [{"phase": "Running"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.35 * 1.0, abs=0.01)  # 0.35

    def test_oom_kill_critical_weight(self) -> None:
        """OOM kill should contribute 0.25 weight as critical failure."""
        metrics = {
            "pod_restarts": [{"value": 0}],
            "oom_kills": [{"pod": "amf-1", "value": 1}],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [{"phase": "Running"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.25 * 1.0, abs=0.01)  # 0.25

    def test_pod_status_failed(self) -> None:
        """Failed pod status should contribute 0.20 weight."""
        metrics = {
            "pod_restarts": [{"value": 0}],
            "oom_kills": [],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [{"phase": "Failed"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.20 * 1.0, abs=0.01)  # 0.20

    def test_pod_status_pending(self) -> None:
        """Pending pod status should contribute 0.20 * 0.6 weight."""
        metrics = {
            "pod_restarts": [{"value": 0}],
            "oom_kills": [],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [{"phase": "Pending"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.20 * 0.6, abs=0.01)  # 0.12

    def test_memory_saturation(self) -> None:
        """Memory >90% should contribute 0.20 weight."""
        metrics = {
            "pod_restarts": [{"value": 0}],
            "oom_kills": [],
            "cpu_usage": [{"value": 0.5}],
            "memory_percent": [{"value": 95}],
            "pod_status": [{"phase": "Running"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.20 * 1.0, abs=0.01)  # 0.20

    def test_cpu_saturation(self) -> None:
        """CPU >1.0 core should contribute 0.20 * 0.8 weight."""
        metrics = {
            "pod_restarts": [{"value": 0}],
            "oom_kills": [],
            "cpu_usage": [{"value": 1.5}],
            "memory_percent": [{"value": 50}],
            "pod_status": [{"phase": "Running"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.20 * 0.8, abs=0.01)  # 0.16

    def test_combined_issues_accumulate(self) -> None:
        """Multiple issues should accumulate weighted scores."""
        # OOM (0.25) + Memory saturation (0.20) = 0.45
        metrics = {
            "pod_restarts": [{"value": 0}],
            "oom_kills": [{"value": 1}],
            "cpu_usage": [{"value": 0.5}],
            "memory_percent": [{"value": 95}],
            "pod_status": [{"phase": "Running"}],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.25 + 0.20, abs=0.01)  # 0.45

    def test_max_score_capped_at_one(self) -> None:
        """Score should never exceed 1.0."""
        # All factors maxed out would exceed 1.0
        metrics = {
            "pod_restarts": [{"value": 10}],  # 0.35
            "oom_kills": [{"value": 5}],      # 0.25
            "cpu_usage": [{"value": 2.0}],    # (part of 0.20)
            "memory_percent": [{"value": 99}], # (part of 0.20)
            "pod_status": [{"phase": "Failed"}],  # 0.20
        }

        score = compute_infrastructure_score(metrics)
        assert score <= 1.0

    def test_empty_metrics_returns_low_score(self) -> None:
        """Empty metrics should return low score (no evidence)."""
        metrics = {
            "pod_restarts": [],
            "oom_kills": [],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [],
        }

        score = compute_infrastructure_score(metrics)
        assert score == pytest.approx(0.0, abs=0.01)

    def test_missing_metric_categories(self) -> None:
        """Missing metric categories should be handled gracefully."""
        metrics = {
            "pod_restarts": [{"value": 2}],
            # Missing other categories
        }

        # Should not raise, should handle missing keys
        score = compute_infrastructure_score(metrics)
        assert 0.0 <= score <= 1.0


class TestInfraPrometheusQueries:
    """Tests for build_infra_queries() function."""

    def test_queries_defined(self) -> None:
        """Ensure Prometheus queries are returned."""
        assert len(build_infra_queries("5g-core")) > 0

    def test_queries_are_strings(self) -> None:
        """All queries should be non-empty strings."""
        for query in build_infra_queries("5g-core"):
            assert isinstance(query, str)
            assert len(query) > 0

    def test_queries_target_5g_core_namespace(self) -> None:
        """Queries should target 5g-core namespace by default."""
        for query in build_infra_queries("5g-core"):
            assert "5g-core" in query or "namespace" in query

    def test_queries_use_custom_namespace(self) -> None:
        """Queries should use the namespace passed as argument."""
        for query in build_infra_queries("my-5g-ns"):
            assert "my-5g-ns" in query


# ===========================================================================
# NEW: Tests for stub functions
# ===========================================================================


class TestParseTimestamp:
    """Tests for parse_timestamp(). Returns Unix epoch (int or float)."""

    def test_parses_iso8601_utc(self) -> None:
        """Standard Alertmanager ISO 8601 Z-suffix timestamp."""
        result = parse_timestamp("2026-02-15T10:00:00Z")
        # 2026-02-15T10:00:00Z -> known Unix epoch
        assert isinstance(result, (int, float))
        assert result == pytest.approx(1771149600, abs=1)

    def test_parses_iso8601_with_fractional_seconds(self) -> None:
        """Alertmanager sometimes includes fractional seconds."""
        result = parse_timestamp("2026-02-15T10:00:00.123Z")
        assert isinstance(result, (int, float))
        assert result == pytest.approx(1771149600.123, abs=1)

    def test_parses_iso8601_with_offset(self) -> None:
        """Timezone offset format (+00:00)."""
        result = parse_timestamp("2026-02-15T10:00:00+00:00")
        assert isinstance(result, (int, float))
        assert result == pytest.approx(1771149600, abs=1)

    def test_returns_numeric_for_arithmetic(self) -> None:
        """Result must support arithmetic (used as alert_time - 300)."""
        result = parse_timestamp("2026-02-15T10:00:00Z")
        window_start = result - 300
        window_end = result + 60
        assert window_end > window_start

    def test_invalid_timestamp_raises(self) -> None:
        """Non-ISO string should raise ValueError."""
        with pytest.raises((ValueError, TypeError)):
            parse_timestamp("not-a-timestamp")

    def test_empty_string_raises(self) -> None:
        """Empty string should raise."""
        with pytest.raises((ValueError, TypeError)):
            parse_timestamp("")


class TestExtractNfsFromAlert:
    """Tests for extract_nfs_from_alert(). Returns list of NF names."""

    def test_extracts_nf_from_labels(self) -> None:
        """Should extract NF from alert labels['nf']."""
        alert: dict[str, Any] = {
            "labels": {"alertname": "RegistrationFailures", "nf": "amf"},
            "startsAt": "2026-02-15T10:00:00Z",
        }
        result = extract_nfs_from_alert(alert)
        assert isinstance(result, list)
        assert len(result) >= 1
        # NF names should be normalized (case-insensitive match)
        assert any(nf.lower() == "amf" for nf in result)

    def test_extracts_multiple_nfs_from_comma_separated(self) -> None:
        """Some alerts have comma-separated NF list."""
        alert: dict[str, Any] = {
            "labels": {"alertname": "ServiceMeshFailure", "nf": "amf,ausf,udm"},
            "startsAt": "2026-02-15T10:00:00Z",
        }
        result = extract_nfs_from_alert(alert)
        assert len(result) >= 3
        nf_lower = [nf.lower() for nf in result]
        assert "amf" in nf_lower
        assert "ausf" in nf_lower
        assert "udm" in nf_lower

    def test_extracts_nf_from_pod_label(self) -> None:
        """Should fallback to extracting NF from pod name label."""
        alert: dict[str, Any] = {
            "labels": {
                "alertname": "PodRestarting",
                "pod": "smf-deployment-abc123",
                "namespace": "5g-core",
            },
            "startsAt": "2026-02-15T10:00:00Z",
        }
        result = extract_nfs_from_alert(alert)
        assert len(result) >= 1
        assert any(nf.lower() == "smf" for nf in result)

    def test_returns_empty_list_when_no_nf_labels(self) -> None:
        """Should return empty list if no NF can be extracted."""
        alert: dict[str, Any] = {
            "labels": {"alertname": "GenericAlert"},
            "startsAt": "2026-02-15T10:00:00Z",
        }
        result = extract_nfs_from_alert(alert)
        assert isinstance(result, list)
        assert len(result) == 0

    def test_handles_missing_labels_key(self) -> None:
        """Gracefully handle alert with no labels."""
        alert: dict[str, Any] = {"startsAt": "2026-02-15T10:00:00Z"}
        result = extract_nfs_from_alert(alert)
        assert isinstance(result, list)
        assert len(result) == 0

    def test_uses_sample_alert_fixture(self, sample_alert: dict[str, Any]) -> None:
        """Should work with the standard sample_alert from conftest."""
        result = extract_nfs_from_alert(sample_alert)
        assert len(result) >= 1
        assert any(nf.lower() == "amf" for nf in result)


class TestExtractRestartCounts:
    """Tests for extract_restart_counts(). Returns {pod: count} mapping."""

    def test_returns_dict(self, healthy_metrics: dict[str, Any]) -> None:
        result = extract_restart_counts(healthy_metrics)
        assert isinstance(result, dict)

    def test_includes_all_pods(self, healthy_metrics: dict[str, Any]) -> None:
        """Should include every pod from pod_restarts, even zero-restart ones."""
        result = extract_restart_counts(healthy_metrics)
        assert "amf-deployment-abc" in result
        assert "ausf-deployment-def" in result

    def test_zero_restarts(self, healthy_metrics: dict[str, Any]) -> None:
        """Zero-restart pods should have value 0."""
        result = extract_restart_counts(healthy_metrics)
        assert result["amf-deployment-abc"] == 0

    def test_nonzero_restarts(self, degraded_metrics: dict[str, Any]) -> None:
        """Pods with restarts should reflect the correct count."""
        result = extract_restart_counts(degraded_metrics)
        assert result["amf-deployment-abc"] == 4
        assert result["udm-deployment-ghi"] == 7

    def test_empty_pod_restarts(self) -> None:
        """Empty pod_restarts list returns empty dict."""
        metrics: dict[str, Any] = {"pod_restarts": []}
        result = extract_restart_counts(metrics)
        assert result == {}

    def test_missing_pod_restarts_key(self) -> None:
        """Missing 'pod_restarts' key returns empty dict."""
        result = extract_restart_counts({})
        assert result == {}


class TestExtractOomEvents:
    """Tests for extract_oom_events(). Returns {pod: oom_count}."""

    def test_returns_dict(self) -> None:
        metrics: dict[str, Any] = {"oom_kills": []}
        result = extract_oom_events(metrics)
        assert isinstance(result, dict)

    def test_no_oom_events(self, healthy_metrics: dict[str, Any]) -> None:
        """Healthy infra with no OOMs should return empty dict."""
        result = extract_oom_events(healthy_metrics)
        assert len(result) == 0

    def test_oom_events_present(self, degraded_metrics: dict[str, Any]) -> None:
        """Should capture pods that experienced OOM kills."""
        result = extract_oom_events(degraded_metrics)
        assert "udm-deployment-ghi" in result
        assert result["udm-deployment-ghi"] == 2

    def test_multiple_oom_pods(self) -> None:
        """Should handle multiple pods with OOM kills."""
        metrics: dict[str, Any] = {
            "oom_kills": [
                {"pod": "amf-1", "container": "amf", "value": 1},
                {"pod": "udm-1", "container": "udm", "value": 3},
            ],
        }
        result = extract_oom_events(metrics)
        assert result["amf-1"] == 1
        assert result["udm-1"] == 3

    def test_missing_oom_kills_key(self) -> None:
        """Missing key returns empty dict."""
        result = extract_oom_events({})
        assert result == {}


class TestExtractResourceMetrics:
    """Tests for extract_resource_metrics(). Returns {pod: {cpu, memory_percent}}."""

    def test_returns_dict(self, healthy_metrics: dict[str, Any]) -> None:
        result = extract_resource_metrics(healthy_metrics)
        assert isinstance(result, dict)

    def test_healthy_resources(self, healthy_metrics: dict[str, Any]) -> None:
        """Should report CPU and memory for each pod."""
        result = extract_resource_metrics(healthy_metrics)
        assert "amf-deployment-abc" in result
        entry = result["amf-deployment-abc"]
        assert "cpu" in entry
        assert "memory_percent" in entry
        assert entry["cpu"] == pytest.approx(0.3, abs=0.01)
        assert entry["memory_percent"] == pytest.approx(45, abs=0.1)

    def test_saturated_resources(self, degraded_metrics: dict[str, Any]) -> None:
        """Should correctly report high resource usage."""
        result = extract_resource_metrics(degraded_metrics)
        amf = result["amf-deployment-abc"]
        assert amf["cpu"] == pytest.approx(1.5, abs=0.01)
        assert amf["memory_percent"] == pytest.approx(92, abs=0.1)

    def test_cpu_only(self) -> None:
        """If only CPU data is available, memory should be absent or 0."""
        metrics: dict[str, Any] = {
            "cpu_usage": [{"pod": "amf-1", "value": 0.5}],
            "memory_percent": [],
        }
        result = extract_resource_metrics(metrics)
        assert "amf-1" in result
        assert result["amf-1"]["cpu"] == pytest.approx(0.5, abs=0.01)

    def test_memory_only(self) -> None:
        """If only memory data is available, CPU should be absent or 0."""
        metrics: dict[str, Any] = {
            "cpu_usage": [],
            "memory_percent": [{"pod": "amf-1", "value": 80}],
        }
        result = extract_resource_metrics(metrics)
        assert "amf-1" in result
        assert result["amf-1"]["memory_percent"] == pytest.approx(80, abs=0.1)

    def test_empty_metrics(self) -> None:
        """Empty lists return empty dict."""
        metrics: dict[str, Any] = {"cpu_usage": [], "memory_percent": []}
        result = extract_resource_metrics(metrics)
        assert result == {}

    def test_missing_keys(self) -> None:
        """Missing keys return empty dict."""
        result = extract_resource_metrics({})
        assert result == {}


class TestExtractNodeStatus:
    """Tests for extract_node_status(). Returns {pod: phase_string}."""

    def test_returns_dict(self, healthy_metrics: dict[str, Any]) -> None:
        result = extract_node_status(healthy_metrics)
        assert isinstance(result, dict)

    def test_all_running(self, healthy_metrics: dict[str, Any]) -> None:
        """All healthy pods should report Running phase."""
        result = extract_node_status(healthy_metrics)
        assert result["amf-deployment-abc"] == "Running"
        assert result["ausf-deployment-def"] == "Running"

    def test_mixed_statuses(self, degraded_metrics: dict[str, Any]) -> None:
        """Should capture various pod phases."""
        result = extract_node_status(degraded_metrics)
        assert result["amf-deployment-abc"] == "Running"
        assert result["udm-deployment-ghi"] == "CrashLoopBackOff"

    def test_empty_pod_status(self) -> None:
        """Empty pod_status list returns empty dict."""
        metrics: dict[str, Any] = {"pod_status": []}
        result = extract_node_status(metrics)
        assert result == {}

    def test_missing_pod_status_key(self) -> None:
        """Missing key returns empty dict."""
        result = extract_node_status({})
        assert result == {}


class TestCountConcurrentFailures:
    """Tests for count_concurrent_failures(). Returns int count of failing pods."""

    def test_returns_int(self, healthy_metrics: dict[str, Any]) -> None:
        result = count_concurrent_failures(healthy_metrics)
        assert isinstance(result, int)

    def test_no_failures(self, healthy_metrics: dict[str, Any]) -> None:
        """Healthy metrics should have zero concurrent failures."""
        result = count_concurrent_failures(healthy_metrics)
        assert result == 0

    def test_multiple_failures(self, degraded_metrics: dict[str, Any]) -> None:
        """Should count distinct pods with any issue.

        degraded_metrics has:
        - amf: 4 restarts + high CPU + high memory -> failing
        - ausf: healthy
        - udm: 7 restarts + OOM + CrashLoopBackOff -> failing
        """
        result = count_concurrent_failures(degraded_metrics)
        assert result >= 2  # at least amf + udm

    def test_single_failure(self) -> None:
        """One pod with restarts, others healthy."""
        metrics: dict[str, Any] = {
            "pod_restarts": [
                {"pod": "amf-1", "value": 3},
                {"pod": "ausf-1", "value": 0},
            ],
            "oom_kills": [],
            "pod_status": [
                {"pod": "amf-1", "phase": "Running"},
                {"pod": "ausf-1", "phase": "Running"},
            ],
            "cpu_usage": [],
            "memory_percent": [],
        }
        result = count_concurrent_failures(metrics)
        assert result == 1

    def test_empty_metrics(self) -> None:
        """Empty metrics = zero failures."""
        result = count_concurrent_failures({})
        assert result == 0


class TestExtractCriticalEvents:
    """Tests for extract_critical_events(). Returns list of event dicts."""

    def test_returns_list(self, healthy_metrics: dict[str, Any]) -> None:
        result = extract_critical_events(healthy_metrics)
        assert isinstance(result, list)

    def test_no_critical_events_when_healthy(
        self, healthy_metrics: dict[str, Any]
    ) -> None:
        """Healthy infra should produce no critical events."""
        result = extract_critical_events(healthy_metrics)
        assert len(result) == 0

    def test_oom_kill_is_critical(self) -> None:
        """OOM kills should be flagged as critical events."""
        metrics: dict[str, Any] = {
            "pod_restarts": [],
            "oom_kills": [{"pod": "amf-1", "container": "amf", "value": 1}],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [],
        }
        result = extract_critical_events(metrics)
        assert len(result) >= 1
        event = result[0]
        assert "pod" in event
        assert event["pod"] == "amf-1"

    def test_high_restarts_is_critical(self) -> None:
        """Pods with >5 restarts should be flagged as critical."""
        metrics: dict[str, Any] = {
            "pod_restarts": [{"pod": "udm-1", "container": "udm", "value": 8}],
            "oom_kills": [],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [],
        }
        result = extract_critical_events(metrics)
        assert len(result) >= 1
        assert any(e["pod"] == "udm-1" for e in result)

    def test_failed_pod_is_critical(self) -> None:
        """Failed pod status should be flagged as critical."""
        metrics: dict[str, Any] = {
            "pod_restarts": [],
            "oom_kills": [],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [{"pod": "smf-1", "phase": "Failed"}],
        }
        result = extract_critical_events(metrics)
        assert len(result) >= 1
        assert any(e["pod"] == "smf-1" for e in result)

    def test_multiple_critical_events(
        self, degraded_metrics: dict[str, Any]
    ) -> None:
        """Multiple failures should each produce a critical event."""
        result = extract_critical_events(degraded_metrics)
        # udm has OOM + high restarts; should produce at least 1 event
        assert len(result) >= 1

    def test_event_has_type_field(self) -> None:
        """Each critical event should have a 'type' field describing the issue."""
        metrics: dict[str, Any] = {
            "pod_restarts": [],
            "oom_kills": [{"pod": "amf-1", "container": "amf", "value": 1}],
            "cpu_usage": [],
            "memory_percent": [],
            "pod_status": [],
        }
        result = extract_critical_events(metrics)
        assert len(result) >= 1
        assert "type" in result[0]

    def test_empty_metrics(self) -> None:
        """Empty metrics = no critical events."""
        result = extract_critical_events({})
        assert result == []


class TestInfraAgentFunction:
    """Tests for infra_agent() entry point with mocked MCP client."""

    def test_sets_infra_checked_true(
        self, sample_initial_state: TriageState
    ) -> None:
        """infra_agent must set infra_checked = True."""
        state = infra_agent(sample_initial_state)
        assert state["infra_checked"] is True

    def test_sets_infra_score(self, sample_initial_state: TriageState) -> None:
        """infra_agent must set infra_score between 0.0 and 1.0."""
        state = infra_agent(sample_initial_state)
        assert 0.0 <= state["infra_score"] <= 1.0

    def test_sets_infra_findings_dict(
        self, sample_initial_state: TriageState
    ) -> None:
        """infra_agent returns a compressed infra_findings dict.

        When MCP is not wired, metrics are empty → infra_score == 0.0 →
        compress_infra_findings_for_agent returns the healthy sentinel dict.
        """
        state = infra_agent(sample_initial_state)
        findings = state["infra_findings"]
        assert isinstance(findings, dict)
        # Empty metrics → healthy sentinel
        assert findings == {"status": "all_pods_healthy"}


def test_infra_agent_prometheus_failure(sample_initial_state: TriageState) -> None:
    """When parse_timestamp raises, infra_agent returns safe defaults."""
    from unittest.mock import patch

    with patch(
        "triage_agent.agents.infra_agent.parse_timestamp",
        side_effect=ValueError("bad timestamp"),
    ):
        result = infra_agent(sample_initial_state)

    assert result["infra_checked"] is True
    assert result["infra_score"] == 0.0
    assert result["infra_findings"] is None


# ===========================================================================
# NEW: Tests for Prometheus wiring (Tasks 1-3)
# ===========================================================================


class TestFetchPrometheusMetrics:
    """Tests for _fetch_prometheus_metrics() direct HTTP helper."""

    @pytest.mark.asyncio
    async def test_fetch_prometheus_metrics_returns_parsed_data(self) -> None:
        """_fetch_prometheus_metrics returns dict keyed by report label."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from triage_agent.agents.infra_agent import _fetch_prometheus_metrics

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {"report": "pod_restarts", "pod": "amf-abc"},
                        "value": [1700000000, "3"],
                    }
                ],
            },
        }

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_resp)

        with patch("triage_agent.agents.infra_agent.httpx.AsyncClient", return_value=mock_client):
            result = await _fetch_prometheus_metrics(["amf"], 1700000000, 1700001000)

        assert isinstance(result, dict)
        assert result  # non-empty

    @pytest.mark.asyncio
    async def test_fetch_prometheus_metrics_returns_empty_on_failure(self) -> None:
        """_fetch_prometheus_metrics returns {} when Prometheus is unreachable."""
        from unittest.mock import AsyncMock, patch

        from triage_agent.agents.infra_agent import _fetch_prometheus_metrics

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=Exception("connection refused"))

        with patch("triage_agent.agents.infra_agent.httpx.AsyncClient", return_value=mock_client):
            result = await _fetch_prometheus_metrics(["amf"], 1700000000, 1700001000)

        assert isinstance(result, dict)
        assert result == {}


class TestReplicaStatus:
    """Tests for replica-absence PromQL query and extract_replica_status."""

    def test_build_infra_queries_unchanged(self) -> None:
        """build_infra_queries still takes core_namespace and returns 5+ queries."""
        queries = build_infra_queries("5g-core")
        assert len(queries) >= 5

    def test_extract_replica_status_zero_replicas(self) -> None:
        """extract_replica_status returns deployment name when replicas == 0."""
        from triage_agent.agents.infra_agent import extract_replica_status

        metrics = {
            "replicas_available": [
                {"metric": {"deployment": "amf", "report": "replicas_available"}, "value": [0, "0"]},
            ]
        }
        absent = extract_replica_status(metrics)
        assert "amf" in absent

    def test_extract_replica_status_healthy(self) -> None:
        """extract_replica_status excludes deployments with replicas > 0."""
        from triage_agent.agents.infra_agent import extract_replica_status

        metrics = {
            "replicas_available": [
                {"metric": {"deployment": "amf", "report": "replicas_available"}, "value": [0, "2"]},
            ]
        }
        absent = extract_replica_status(metrics)
        assert "amf" not in absent

    def test_extract_replica_status_empty(self) -> None:
        """extract_replica_status returns empty set when no replica data."""
        from triage_agent.agents.infra_agent import extract_replica_status

        absent = extract_replica_status({})
        assert absent == set()

    def test_compute_infrastructure_score_zero_replicas(self) -> None:
        """score == 1.0 when a DAG NF has 0 available replicas (short-circuit)."""
        metrics = {
            "replicas_available": [
                {"metric": {"deployment": "amf", "report": "replicas_available"}, "value": [0, "0"]},
            ]
        }
        score = compute_infrastructure_score(metrics, ["AMF"])
        assert score == pytest.approx(1.0), "absent deployment must score 1.0 (definitive infrastructure event)"

    def test_compute_infrastructure_score_nonzero_replicas_unchanged(self) -> None:
        """Existing 0-restart, running-pod behavior unchanged when replicas > 0."""
        metrics = {
            "pod_restarts": [{"pod": "amf-1", "value": 0}],
            "oom_kills": [],
            "cpu_usage": [{"pod": "amf-1", "value": 0.3}],
            "memory_percent": [{"pod": "amf-1", "value": 50}],
            "pod_status": [{"pod": "amf-1", "phase": "Running"}],
            "replicas_available": [
                {"metric": {"deployment": "amf"}, "value": [0, "2"]},
            ],
        }
        score = compute_infrastructure_score(metrics, ["AMF"])
        assert score == pytest.approx(0.0, abs=0.01)


class TestInfraAgentZeroReplica:
    """Tests for infra_agent() entry point with 0-replica scenario."""

    def test_infra_agent_scores_zero_replica_nf(
        self, monkeypatch: pytest.MonkeyPatch, sample_initial_state: TriageState
    ) -> None:
        """infra_score > 0 when a DAG NF has 0 available replicas."""
        import sys

        # Use sys.modules to get the actual module object, not the function shadowed
        # by agents/__init__.py which imports `infra_agent` (the function) as an
        # attribute of the package, masking the module in attribute-chain lookups.
        infra_module = sys.modules["triage_agent.agents.infra_agent"]

        async def fake_fetch(nfs: list[str], start: int, end: int) -> dict:
            return {
                "replicas_available": [
                    {
                        "metric": {"deployment": "amf", "report": "replicas_available"},
                        "value": [0, "0"],
                    },
                ]
            }

        monkeypatch.setattr(infra_module, "_fetch_prometheus_metrics", fake_fetch)
        result = infra_agent(sample_initial_state)
        assert result["infra_score"] >= 0.80, "Expected infra_score >= 0.80 for 0-replica AMF (infrastructure layer)"
        assert result["infra_checked"] is True

