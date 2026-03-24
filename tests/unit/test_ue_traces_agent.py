"""Tests for UeTracesAgent - test-first development.

Covers:
  - extract_unique_imsis: finds IMSI format "imsi-<15 digits>"
  - per_imsi_logql: builds LogQL for a single IMSI
  - contract_imsi_trace: contracts raw logs into structured trace
  - ingest_traces_to_memgraph: ingests traces into Memgraph
  - run_deviation_detection_for_dag: detects deviations via Cypher
  - ue_traces_agent: entry point, updates state
  - Edge cases: no IMSIs found, Memgraph connection failure
"""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from triage_agent.agents.ue_traces_agent import (
    contract_imsi_trace,
    extract_unique_imsis,
    ingest_traces_to_memgraph,
    per_imsi_logql,
    run_deviation_detection_for_dag,
    ue_traces_agent,
)
from triage_agent.state import TriageState

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def discovery_logs() -> list[dict[str, Any]]:
    """Sample Loki log entries containing IMSI references."""
    return [
        {
            "timestamp": 1708000000,
            "message": "Registration Request received from imsi-001010123456789",
            "pod": "amf-deployment-abc123",
            "level": "INFO",
        },
        {
            "timestamp": 1708000001,
            "message": "Authentication initiated for imsi-001010123456789",
            "pod": "ausf-deployment-def456",
            "level": "INFO",
        },
        {
            "timestamp": 1708000002,
            "message": "Registration Request received from imsi-001010987654321",
            "pod": "amf-deployment-abc123",
            "level": "INFO",
        },
        {
            "timestamp": 1708000003,
            "message": "No IMSI in this log line",
            "pod": "nrf-deployment-xyz",
            "level": "INFO",
        },
    ]


@pytest.fixture
def single_imsi_trace_logs() -> list[dict[str, Any]]:
    """Raw Loki logs for a single IMSI trace."""
    return [
        {
            "timestamp": 1708000000,
            "message": "Registration Request from imsi-001010123456789",
            "pod": "amf-deployment-abc",
            "level": "INFO",
        },
        {
            "timestamp": 1708000001,
            "message": "Authentication request sent to AUSF for imsi-001010123456789",
            "pod": "amf-deployment-abc",
            "level": "INFO",
        },
        {
            "timestamp": 1708000002,
            "message": "Authentication response received for imsi-001010123456789",
            "pod": "ausf-deployment-def",
            "level": "INFO",
        },
    ]


@pytest.fixture
def contracted_traces() -> list[dict[str, Any]]:
    """Pre-built contracted trace dicts for ingestion tests."""
    return [
        {
            "imsi": "001010123456789",
            "events": [
                {
                    "timestamp": 1708000000,
                    "nf": "amf",
                    "action": "Registration Request",
                    "message": "Registration Request from imsi-001010123456789",
                },
                {
                    "timestamp": 1708000001,
                    "nf": "amf",
                    "action": "Authentication request",
                    "message": "Authentication request sent to AUSF",
                },
            ],
        },
        {
            "imsi": "001010987654321",
            "events": [
                {
                    "timestamp": 1708000002,
                    "nf": "amf",
                    "action": "Registration Request",
                    "message": "Registration Request from imsi-001010987654321",
                },
            ],
        },
    ]


# ===========================================================================
# extract_unique_imsis
# ===========================================================================


class TestExtractUniqueImsis:
    """Tests for extract_unique_imsis().

    Contract: Scans log messages for IMSI pattern 'imsi-<15 digits>',
    returns deduplicated list of IMSI strings (digits only).
    """

    def test_finds_imsi_in_log_messages(
        self, discovery_logs: list[dict[str, Any]]
    ) -> None:
        """Should extract IMSIs from 'imsi-<15digits>' format in messages."""
        result = extract_unique_imsis(discovery_logs)
        assert "001010123456789" in result

    def test_finds_multiple_distinct_imsis(
        self, discovery_logs: list[dict[str, Any]]
    ) -> None:
        """Should find all distinct IMSIs across log entries."""
        result = extract_unique_imsis(discovery_logs)
        assert "001010123456789" in result
        assert "001010987654321" in result
        assert len(result) == 2

    def test_deduplicates_repeated_imsis(
        self, discovery_logs: list[dict[str, Any]]
    ) -> None:
        """IMSI appearing in multiple log lines should appear once."""
        result = extract_unique_imsis(discovery_logs)
        # 001010123456789 appears in two log entries
        assert result.count("001010123456789") == 1

    def test_returns_list_of_strings(
        self, discovery_logs: list[dict[str, Any]]
    ) -> None:
        result = extract_unique_imsis(discovery_logs)
        assert isinstance(result, list)
        for imsi in result:
            assert isinstance(imsi, str)

    def test_imsi_is_15_digits(
        self, discovery_logs: list[dict[str, Any]]
    ) -> None:
        """Each extracted IMSI should be exactly 15 digits."""
        result = extract_unique_imsis(discovery_logs)
        for imsi in result:
            assert len(imsi) == 15
            assert imsi.isdigit()

    def test_no_imsis_in_logs(self) -> None:
        """Logs without any IMSI references should return empty list."""
        logs = [
            {"timestamp": 0, "message": "No subscriber info here", "pod": "amf-1", "level": "INFO"},
            {"timestamp": 1, "message": "Generic error occurred", "pod": "ausf-1", "level": "ERROR"},
        ]
        result = extract_unique_imsis(logs)
        assert result == []

    def test_empty_logs_list(self) -> None:
        """Empty input should return empty list."""
        result = extract_unique_imsis([])
        assert result == []

    def test_case_insensitive_imsi_prefix(self) -> None:
        """Should match IMSI- and imsi- (case-insensitive prefix)."""
        logs = [
            {"timestamp": 0, "message": "Request for IMSI-001010111111111", "pod": "amf-1", "level": "INFO"},
        ]
        result = extract_unique_imsis(logs)
        assert "001010111111111" in result

    def test_ignores_short_digit_sequences(self) -> None:
        """Should not match digit sequences shorter than 15 digits."""
        logs = [
            {"timestamp": 0, "message": "Error code imsi-12345", "pod": "amf-1", "level": "ERROR"},
        ]
        result = extract_unique_imsis(logs)
        assert result == []


# ===========================================================================
# per_imsi_logql
# ===========================================================================


class TestPerImsiLogql:
    """Tests for per_imsi_logql(). Builds LogQL for a specific IMSI."""

    def test_returns_string(self) -> None:
        result = per_imsi_logql("001010123456789")
        assert isinstance(result, str)

    def test_contains_imsi(self) -> None:
        """Query must filter for the specific IMSI."""
        result = per_imsi_logql("001010123456789")
        assert "001010123456789" in result

    def test_targets_5g_core_namespace(self) -> None:
        """Query should target 5g-core namespace."""
        result = per_imsi_logql("001010123456789")
        assert "5g-core" in result

    def test_is_valid_logql(self) -> None:
        """Should look like a LogQL stream selector + filter."""
        result = per_imsi_logql("001010123456789")
        assert "{" in result and "}" in result


# ===========================================================================
# contract_imsi_trace
# ===========================================================================


class TestContractImsiTrace:
    """Tests for contract_imsi_trace().

    Contract: Takes raw log entries and IMSI, returns structured trace dict
    with 'imsi' and 'events' keys for Memgraph ingestion.
    """

    def test_returns_dict(
        self, single_imsi_trace_logs: list[dict[str, Any]]
    ) -> None:
        result = contract_imsi_trace(single_imsi_trace_logs, "001010123456789")
        assert isinstance(result, dict)

    def test_has_imsi_field(
        self, single_imsi_trace_logs: list[dict[str, Any]]
    ) -> None:
        result = contract_imsi_trace(single_imsi_trace_logs, "001010123456789")
        assert result["imsi"] == "001010123456789"

    def test_has_events_list(
        self, single_imsi_trace_logs: list[dict[str, Any]]
    ) -> None:
        result = contract_imsi_trace(single_imsi_trace_logs, "001010123456789")
        assert isinstance(result["events"], list)
        assert len(result["events"]) == len(single_imsi_trace_logs)

    def test_events_have_timestamp(
        self, single_imsi_trace_logs: list[dict[str, Any]]
    ) -> None:
        result = contract_imsi_trace(single_imsi_trace_logs, "001010123456789")
        for event in result["events"]:
            assert "timestamp" in event

    def test_events_have_nf(
        self, single_imsi_trace_logs: list[dict[str, Any]]
    ) -> None:
        """Each event should have 'nf' extracted from pod name."""
        result = contract_imsi_trace(single_imsi_trace_logs, "001010123456789")
        nfs = [e["nf"] for e in result["events"]]
        assert "amf" in nfs
        assert "ausf" in nfs

    def test_events_have_message(
        self, single_imsi_trace_logs: list[dict[str, Any]]
    ) -> None:
        result = contract_imsi_trace(single_imsi_trace_logs, "001010123456789")
        for event in result["events"]:
            assert "message" in event
            assert isinstance(event["message"], str)

    def test_events_ordered_by_timestamp(
        self, single_imsi_trace_logs: list[dict[str, Any]]
    ) -> None:
        """Events should be ordered chronologically."""
        result = contract_imsi_trace(single_imsi_trace_logs, "001010123456789")
        timestamps = [e["timestamp"] for e in result["events"]]
        assert timestamps == sorted(timestamps)

    def test_empty_logs_returns_empty_events(self) -> None:
        result = contract_imsi_trace([], "001010123456789")
        assert result["imsi"] == "001010123456789"
        assert result["events"] == []


def test_contract_imsi_trace_includes_order() -> None:
    from triage_agent.agents.ue_traces_agent import contract_imsi_trace
    raw = [
        {"timestamp": 100, "pod": "amf-abc", "message": "SVC_REQUEST"},
        {"timestamp": 90,  "pod": "smf-xyz", "message": "PDU_SESSION"},
    ]
    trace = contract_imsi_trace(raw, "123456789012345")
    assert trace["imsi"] == "123456789012345"
    # Events sorted by timestamp
    assert trace["events"][0]["timestamp"] == 90
    assert trace["events"][1]["timestamp"] == 100
    # Each event must have order, message, nf, timestamp
    for i, event in enumerate(trace["events"]):
        assert event["order"] == i
        assert "message" in event
        assert "nf" in event
        assert "timestamp" in event


# ===========================================================================
# ingest_traces_to_memgraph
# ===========================================================================


class TestIngestTracesToMemgraph:
    """Tests for ingest_traces_to_memgraph().

    Uses mock_memgraph to verify Memgraph interactions.
    """

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    def test_calls_ingest_for_each_trace(
        self,
        mock_get_memgraph: MagicMock,
        contracted_traces: list[dict[str, Any]],
    ) -> None:
        """Should call ingest_captured_trace once per trace."""
        mock_conn = MagicMock()
        mock_get_memgraph.return_value = mock_conn

        ingest_traces_to_memgraph(contracted_traces, "test-incident-001")

        assert mock_conn.ingest_captured_trace.call_count == 2

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    def test_passes_correct_params(
        self,
        mock_get_memgraph: MagicMock,
        contracted_traces: list[dict[str, Any]],
    ) -> None:
        """Should pass incident_id, imsi, and events to Memgraph."""
        mock_conn = MagicMock()
        mock_get_memgraph.return_value = mock_conn

        ingest_traces_to_memgraph(contracted_traces, "test-incident-001")

        first_call = mock_conn.ingest_captured_trace.call_args_list[0]
        assert first_call.args[0] == "test-incident-001"  # incident_id
        assert first_call.args[1] == "001010123456789"  # imsi
        assert isinstance(first_call.args[2], list)  # events

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    def test_empty_traces_no_calls(
        self, mock_get_memgraph: MagicMock
    ) -> None:
        """Empty traces list should not call Memgraph."""
        mock_conn = MagicMock()
        mock_get_memgraph.return_value = mock_conn

        ingest_traces_to_memgraph([], "test-incident-001")

        mock_conn.ingest_captured_trace.assert_not_called()

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    def test_memgraph_connection_failure_raises(
        self,
        mock_get_memgraph: MagicMock,
        contracted_traces: list[dict[str, Any]],
    ) -> None:
        """Memgraph connection failure should propagate as exception."""
        mock_conn = MagicMock()
        mock_conn.ingest_captured_trace.side_effect = ConnectionError("Memgraph unavailable")
        mock_get_memgraph.return_value = mock_conn

        with pytest.raises(ConnectionError):
            ingest_traces_to_memgraph(contracted_traces, "test-incident-001")


# ===========================================================================
# run_deviation_detection_for_dag
# ===========================================================================


class TestRunDeviationDetection:
    """Tests for run_deviation_detection_for_dag().

    Uses mock_memgraph to verify Cypher-based deviation queries.
    """

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    def test_returns_list(self, mock_get_memgraph: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = [
            {"imsi": "001010123456789"},
        ]
        mock_conn.detect_deviation.return_value = {
            "deviation_point": 9,
            "expected": "Authentication/Security",
            "actual": "Authentication failed",
            "expected_nf": "AMF",
            "actual_nf": "AMF",
        }
        mock_get_memgraph.return_value = mock_conn

        result = run_deviation_detection_for_dag("test-incident-001", "Registration_General")
        assert isinstance(result, list)

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    def test_returns_deviations_per_imsi(
        self, mock_get_memgraph: MagicMock
    ) -> None:
        """Should return one deviation dict per IMSI that deviates."""
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = [
            {"imsi": "001010123456789"},
            {"imsi": "001010987654321"},
        ]
        mock_conn.detect_deviation.side_effect = [
            {
                "imsi": "001010123456789",
                "deviation_point": 9,
                "expected": "Authentication/Security",
                "actual": "Authentication failed",
                "expected_nf": "AMF",
                "actual_nf": "AMF",
            },
            None,  # second IMSI has no deviation
        ]
        mock_get_memgraph.return_value = mock_conn

        result = run_deviation_detection_for_dag("test-incident-001", "Registration_General")
        assert len(result) == 1
        assert result[0]["deviation_point"] == 9

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    def test_no_traces_returns_empty(
        self, mock_get_memgraph: MagicMock
    ) -> None:
        """No captured traces should return empty deviation list."""
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = []
        mock_get_memgraph.return_value = mock_conn

        result = run_deviation_detection_for_dag("test-incident-001", "Registration_General")
        assert result == []

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    def test_memgraph_failure_raises(
        self, mock_get_memgraph: MagicMock
    ) -> None:
        """Memgraph failure during detection should propagate."""
        mock_conn = MagicMock()
        mock_conn.execute_cypher.side_effect = ConnectionError("Memgraph unavailable")
        mock_get_memgraph.return_value = mock_conn

        with pytest.raises(ConnectionError):
            run_deviation_detection_for_dag("test-incident-001", "Registration_General")


# ===========================================================================
# ue_traces_agent entry point
# ===========================================================================


class TestUeTracesAgent:
    """Tests for ue_traces_agent() entry point.

    Mocks loki_query and Memgraph to test the full orchestration.
    """

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    @patch("triage_agent.agents.ue_traces_agent.loki_query")
    def test_updates_discovered_imsis(
        self,
        mock_loki: MagicMock,
        mock_get_memgraph: MagicMock,
        sample_initial_state: TriageState,
    ) -> None:
        """Should return discovered_imsis with found IMSIs."""
        mock_loki.side_effect = [
            # Discovery pass
            [{"timestamp": 0, "message": "Request from imsi-001010123456789", "pod": "amf-1", "level": "INFO"}],
            # Per-IMSI trace
            [{"timestamp": 0, "message": "trace event", "pod": "amf-1", "level": "INFO"}],
        ]
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = [{"imsi": "001010123456789"}]
        mock_conn.detect_deviation.return_value = None
        mock_get_memgraph.return_value = mock_conn

        state = sample_initial_state
        state["dags"] = [{"name": "registration_general", "all_nfs": ["AMF"]}]
        result = ue_traces_agent(state)

        assert result["discovered_imsis"] == ["001010123456789"]

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    @patch("triage_agent.agents.ue_traces_agent.loki_query")
    def test_sets_traces_ready_true(
        self,
        mock_loki: MagicMock,
        mock_get_memgraph: MagicMock,
        sample_initial_state: TriageState,
    ) -> None:
        """Should return traces_ready = True after ingestion."""
        mock_loki.return_value = []
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = []
        mock_get_memgraph.return_value = mock_conn

        state = sample_initial_state
        state["dags"] = [{"name": "registration_general", "all_nfs": ["AMF"]}]
        result = ue_traces_agent(state)

        assert result["traces_ready"] is True

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    @patch("triage_agent.agents.ue_traces_agent.loki_query")
    def test_sets_trace_deviations(
        self,
        mock_loki: MagicMock,
        mock_get_memgraph: MagicMock,
        sample_initial_state: TriageState,
        sample_trace_deviation: dict[str, Any],
    ) -> None:
        """Should return trace_deviations dict keyed by procedure name."""
        mock_loki.side_effect = [
            [{"timestamp": 0, "message": "imsi-001010123456789", "pod": "amf-1", "level": "INFO"}],
            [{"timestamp": 0, "message": "trace event", "pod": "amf-1", "level": "INFO"}],
        ]
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = [{"imsi": "001010123456789"}]
        mock_conn.detect_deviation.return_value = sample_trace_deviation
        mock_get_memgraph.return_value = mock_conn

        state = sample_initial_state
        state["dags"] = [{"name": "registration_general", "all_nfs": ["AMF"]}]
        result = ue_traces_agent(state)

        assert isinstance(result["trace_deviations"], dict)
        assert "registration_general" in result["trace_deviations"]
        assert len(result["trace_deviations"]["registration_general"]) >= 1

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    @patch("triage_agent.agents.ue_traces_agent.loki_query")
    def test_no_imsis_found(
        self,
        mock_loki: MagicMock,
        mock_get_memgraph: MagicMock,
        sample_initial_state: TriageState,
    ) -> None:
        """When no IMSIs are found, empty lists and traces_ready=True."""
        mock_loki.return_value = [
            {"timestamp": 0, "message": "No subscriber info", "pod": "amf-1", "level": "INFO"},
        ]
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = []
        mock_get_memgraph.return_value = mock_conn

        state = sample_initial_state
        state["dags"] = [{"name": "registration_general", "all_nfs": ["AMF"]}]
        result = ue_traces_agent(state)

        assert result["discovered_imsis"] == []
        assert result["traces_ready"] is True
        assert isinstance(result["trace_deviations"], dict)

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    @patch("triage_agent.agents.ue_traces_agent.loki_query")
    def test_returns_delta_dict(
        self,
        mock_loki: MagicMock,
        mock_get_memgraph: MagicMock,
        sample_initial_state: TriageState,
    ) -> None:
        """ue_traces_agent returns only delta dict keys."""
        mock_loki.return_value = []
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = []
        mock_get_memgraph.return_value = mock_conn

        state = sample_initial_state
        state["dags"] = [{"name": "registration_general", "all_nfs": ["AMF"]}]
        result = ue_traces_agent(state)

        assert isinstance(result, dict)
        assert set(result.keys()) == {"discovered_imsis", "traces_ready", "trace_deviations"}


# ===========================================================================
# Multi-procedure deviation and delta return tests
# ===========================================================================


def test_run_deviation_detection_removed():
    """run_deviation_detection is dead code and must not exist."""
    import triage_agent.agents.ue_traces_agent as uta
    assert not hasattr(uta, "run_deviation_detection"), (
        "run_deviation_detection is dead code — remove it"
    )


class TestUeTracesAgentDeltaReturn:
    """Tests for delta return and per-procedure deviation detection."""

    def test_ue_traces_agent_returns_delta_dict(
        self, sample_initial_state: TriageState, mock_memgraph: MagicMock
    ) -> None:
        """ue_traces_agent returns delta dict, not full state."""
        state = sample_initial_state
        state["dags"] = [{"name": "registration_general", "all_nfs": ["AMF"]}]

        with (
            patch("triage_agent.agents.ue_traces_agent.loki_query", return_value=[]),
            patch("triage_agent.agents.ue_traces_agent.get_memgraph", return_value=mock_memgraph),
        ):
            result = ue_traces_agent(state)

        expected_keys = {"discovered_imsis", "traces_ready", "trace_deviations"}
        assert set(result.keys()) == expected_keys

    def test_deviation_detection_runs_per_procedure(
        self, sample_initial_state: TriageState, mock_memgraph: MagicMock
    ) -> None:
        """trace_deviations is keyed by procedure name, one entry per matched DAG."""
        state = sample_initial_state
        state["dags"] = [
            {"name": "registration_general", "all_nfs": ["AMF"]},
            {"name": "authentication_5g_aka", "all_nfs": ["AUSF"]},
        ]
        mock_memgraph.execute_cypher.return_value = []

        with (
            patch("triage_agent.agents.ue_traces_agent.loki_query", return_value=[]),
            patch("triage_agent.agents.ue_traces_agent.get_memgraph", return_value=mock_memgraph),
        ):
            result = ue_traces_agent(state)

        deviations = result["trace_deviations"]
        assert isinstance(deviations, dict)
        assert "registration_general" in deviations
        assert "authentication_5g_aka" in deviations

    def test_empty_dags_returns_empty_traces(
        self, sample_initial_state: TriageState
    ) -> None:
        """With no DAGs, traces agent returns minimal empty delta dict."""
        state = sample_initial_state
        state["dags"] = []

        result = ue_traces_agent(state)

        assert result["discovered_imsis"] == []
        assert result["traces_ready"] is False
        assert result["trace_deviations"] is None


class TestUeTracesAgentStateLogs:
    """Tests for INC-6: use state.logs for IMSI discovery before Loki query."""

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    @patch("triage_agent.agents.ue_traces_agent.loki_query")
    def test_uses_state_logs_when_available(
        self,
        mock_loki: MagicMock,
        mock_get_memgraph: MagicMock,
        sample_initial_state: TriageState,
    ) -> None:
        """When state.logs contains IMSI-bearing entries, loki_query is NOT called for discovery."""
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = []
        mock_get_memgraph.return_value = mock_conn
        # Per-IMSI trace loki_query (not discovery) returns empty
        mock_loki.return_value = []

        state = sample_initial_state
        state["dags"] = [{"name": "registration_general", "all_nfs": ["AMF"]}]
        state["logs"] = {
            "AMF": [
                {"message": "Registration from imsi-001010123456789 started"},
                {"message": "Normal event"},
            ]
        }

        result = ue_traces_agent(state)

        # Discovery IMSI found from state.logs — loki_query not called at all:
        # discovery is skipped (state.logs present) and per-IMSI traces use
        # the async _build_traces_async path directly, bypassing loki_query.
        assert result["discovered_imsis"] == ["001010123456789"]
        assert mock_loki.call_count == 0

    @patch("triage_agent.agents.ue_traces_agent.get_memgraph")
    @patch("triage_agent.agents.ue_traces_agent.loki_query")
    def test_falls_back_to_loki_when_state_logs_empty(
        self,
        mock_loki: MagicMock,
        mock_get_memgraph: MagicMock,
        sample_initial_state: TriageState,
    ) -> None:
        """When state.logs is empty, loki_query IS called for IMSI discovery."""
        mock_conn = MagicMock()
        mock_conn.execute_cypher.return_value = []
        mock_get_memgraph.return_value = mock_conn
        # Discovery query returns no IMSIs
        mock_loki.return_value = []

        state = sample_initial_state
        state["dags"] = [{"name": "registration_general", "all_nfs": ["AMF"]}]
        state["logs"] = {}

        ue_traces_agent(state)

        # loki_query should be called for discovery (state.logs is empty)
        assert mock_loki.call_count >= 1
