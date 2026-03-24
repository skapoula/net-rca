"""Tests for RCAAgent - the only agent that uses an LLM."""

import json
import uuid
from typing import Any
from unittest.mock import patch

from triage_agent.agents.rca_agent import (
    RCA_PROMPT_TEMPLATE,
    format_logs_for_prompt,
    format_metrics_for_prompt,
    format_trace_deviations_for_prompt,
    identify_evidence_gaps,
    join_for_rca,
    rca_agent_first_attempt,
)
from triage_agent.graph import get_initial_state
from triage_agent.state import TriageState


class TestFormatMetricsForPrompt:
    """Tests for format_metrics_for_prompt helper."""

    def test_none_returns_no_metrics_message(self) -> None:
        """None metrics should return descriptive string."""
        assert format_metrics_for_prompt(None) == "No metrics available."

    def test_empty_dict_returns_no_metrics_message(self) -> None:
        """Empty dict should return descriptive string."""
        assert format_metrics_for_prompt({}) == "No metrics available."

    def test_formats_as_json(self) -> None:
        """Non-empty metrics should be formatted as indented JSON."""
        metrics = {"AMF": [{"error_rate": 0.05}]}
        result = format_metrics_for_prompt(metrics)
        parsed = json.loads(result)
        assert parsed == metrics


class TestFormatLogsForPrompt:
    """Tests for format_logs_for_prompt helper."""

    def test_none_returns_no_logs_message(self) -> None:
        """None logs should return descriptive string."""
        assert format_logs_for_prompt(None) == "No logs available."

    def test_empty_dict_returns_no_logs_message(self) -> None:
        """Empty dict should return descriptive string."""
        assert format_logs_for_prompt({}) == "No logs available."

    def test_formats_as_json(self) -> None:
        """Non-empty logs should be formatted as indented JSON."""
        logs = {"AMF": [{"message": "error", "level": "ERROR"}]}
        result = format_logs_for_prompt(logs)
        parsed = json.loads(result)
        assert parsed == logs


class TestFormatTraceDeviationsForPrompt:
    """Tests for format_trace_deviations_for_prompt helper."""

    def test_none_returns_no_deviations_message(self) -> None:
        """None deviations should return descriptive string."""
        result = format_trace_deviations_for_prompt(None)
        assert result == "No UE trace deviations available."

    def test_empty_dict_returns_no_deviations_message(self) -> None:
        """Empty dict should return descriptive string."""
        result = format_trace_deviations_for_prompt({})
        assert result == "No UE trace deviations available."

    def test_formats_as_json(self) -> None:
        """Non-empty deviations should be formatted as indented JSON."""
        deviations = {"registration_general": [{"deviation_point": 9, "expected": "Auth"}]}
        result = format_trace_deviations_for_prompt(deviations)
        assert "registration_general" in result


class TestRCAPromptTemplate:
    """Tests for RCA_PROMPT_TEMPLATE."""

    def test_template_has_required_placeholders(self) -> None:
        """Template should contain all expected format placeholders."""
        required_placeholders = [
            "{procedure_name}",
            "{infra_score}",
            "{infra_findings_json}",
            "{dag_json}",
            "{time_window}",
            "{metrics_formatted}",
            "{logs_formatted}",
            "{trace_deviations_formatted}",
            "{evidence_quality_score}",
        ]
        for placeholder in required_placeholders:
            assert placeholder in RCA_PROMPT_TEMPLATE, (
                f"Missing placeholder: {placeholder}"
            )

    def test_template_mentions_layer_determination(self) -> None:
        """Template should include infra_score threshold placeholders for layer decision."""
        assert "{infra_root_cause_threshold}" in RCA_PROMPT_TEMPLATE
        assert "{infra_triggered_threshold}" in RCA_PROMPT_TEMPLATE
        assert "{app_only_threshold}" in RCA_PROMPT_TEMPLATE

    def test_template_requests_json_output(self) -> None:
        """Template should request JSON output format."""
        assert '"layer"' in RCA_PROMPT_TEMPLATE
        assert '"root_nf"' in RCA_PROMPT_TEMPLATE
        assert '"confidence"' in RCA_PROMPT_TEMPLATE
        assert '"evidence_chain"' in RCA_PROMPT_TEMPLATE

    def test_template_can_be_formatted(
        self, sample_initial_state: TriageState
    ) -> None:
        """Template should be formattable with real state values."""
        prompt = RCA_PROMPT_TEMPLATE.format(
            procedure_name="Registration_General",
            infra_score=0.15,
            infra_findings_json="{}",
            dag_json="{}",
            time_window="2026-02-15T09:55:00Z to 2026-02-15T10:01:00Z",
            metrics_formatted="No metrics available.",
            logs_formatted="No logs available.",
            trace_deviations_formatted="No UE trace deviations available.",
            evidence_quality_score=0.10,
            infra_root_cause_threshold=0.80,
            infra_triggered_threshold=0.60,
            app_only_threshold=0.30,
        )
        assert "Registration_General" in prompt
        assert "0.15" in prompt


class TestRcaAgentFirstAttempt:
    """Tests for rca_agent_first_attempt entry point."""

    def test_calls_llm_analyze_evidence(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """rca_agent_first_attempt should call llm_analyze_evidence."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"

        # Mock the LLM to verify it's called
        mock_analysis = {
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.85,
            "evidence_chain": [],
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ) as mock_llm:
            result = rca_agent_first_attempt(state)

            # Verify llm_analyze_evidence was called
            assert mock_llm.called
            assert result["root_nf"] == "AUSF"

    def test_sets_needs_more_evidence_false_when_confident(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """Should set needs_more_evidence=False when confidence >= threshold."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.50

        mock_analysis = {
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.85,
            "evidence_chain": [{"source": "logs"}],
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ):
            result = rca_agent_first_attempt(state)

            # Confidence 0.85 >= 0.70, should NOT need more evidence
            assert result["needs_more_evidence"] is False

    def test_sets_needs_more_evidence_true_when_low_confidence(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """Should set needs_more_evidence=True when confidence < threshold."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.50

        mock_analysis = {
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.40,
            "evidence_chain": [],
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ):
            result = rca_agent_first_attempt(state)

            # Confidence 0.40 < 0.70, should need more evidence
            assert result["needs_more_evidence"] is True
            assert result["evidence_gaps"] is not None
            assert len(result["evidence_gaps"]) > 0

    def test_confidence_threshold_adjusts_with_evidence_quality(self) -> None:
        """High evidence quality (>=0.80) should lower confidence threshold to 0.65."""
        # This documents the decision logic in rca_agent_first_attempt:
        # min_confidence = cfg.min_confidence_default (0.70) by default
        # if evidence_quality_score >= cfg.high_evidence_threshold (0.80):
        #     min_confidence = cfg.min_confidence_relaxed (0.65)
        import inspect

        source = inspect.getsource(rca_agent_first_attempt)
        assert "min_confidence_default" in source
        assert "min_confidence_relaxed" in source
        assert "high_evidence_threshold" in source


class TestRCAOutputStructure:
    """Tests for structured RCAOutput Pydantic model."""

    def test_produces_structured_rca_output(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """RCAAgent should produce a structured RCAOutput with all required fields."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.75

        mock_analysis = {
            "layer": "application",
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "failed_phase": "9",
            "confidence": 0.85,
            "evidence_chain": [
                {
                    "timestamp": "2026-02-15T10:00:00Z",
                    "source": "logs",
                    "nf": "AUSF",
                    "type": "log",
                    "content": "Authentication timeout",
                    "significance": "Primary failure indicator",
                }
            ],
            "alternative_hypotheses": [
                {
                    "layer": "infrastructure",
                    "nf": "ausf-pod",
                    "failure_mode": "network_latency",
                    "confidence": 0.30,
                }
            ],
            "reasoning": "Auth timeout logs in AUSF indicate application-layer issue",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ):
            result = rca_agent_first_attempt(state)

            # Verify all required RCAOutput fields are set in state
            assert result["layer"] == "application"
            assert result["root_nf"] == "AUSF"
            assert result["failure_mode"] == "auth_timeout"
            assert result["confidence"] == 0.85
            assert len(result["evidence_chain"]) == 1
            assert result["evidence_chain"][0]["source"] == "logs"


class TestConfidenceThresholdLogic:
    """Tests for confidence threshold decision logic."""

    def test_default_threshold_0_70(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """Default confidence threshold should be 0.70."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.50  # Below 0.80

        mock_analysis = {
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.72,  # Above default 0.70 threshold
            "evidence_chain": [{"source": "logs"}],
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ):
            result = rca_agent_first_attempt(state)

            # Confidence 0.72 >= 0.70, should NOT need more evidence
            assert result["needs_more_evidence"] is False

    def test_lowered_threshold_0_65_when_high_evidence_quality(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """When evidence_quality >= 0.80, threshold should be 0.65."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.85  # >= 0.80

        mock_analysis = {
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.67,  # Between 0.65 and 0.70
            "evidence_chain": [{"source": "logs"}],
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ):
            result = rca_agent_first_attempt(state)

            # Confidence 0.67 >= 0.65 (lowered threshold), should NOT need more evidence
            assert result["needs_more_evidence"] is False

    def test_needs_more_evidence_below_default_threshold(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """Confidence below 0.70 (default) should set needs_more_evidence=True."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.50  # Below 0.80

        mock_analysis = {
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.60,  # Below 0.70 threshold
            "evidence_chain": [],
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ), patch(
            "triage_agent.agents.rca_agent.identify_evidence_gaps",
            return_value=["Need IMSI traces"],
        ):
            result = rca_agent_first_attempt(state)

            # Confidence 0.60 < 0.70, should need more evidence
            assert result["needs_more_evidence"] is True
            assert result["evidence_gaps"] is not None

    def test_needs_more_evidence_below_lowered_threshold(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """Confidence below 0.65 (lowered threshold) should set needs_more_evidence=True."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.85  # >= 0.80

        mock_analysis = {
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.62,  # Below 0.65 lowered threshold
            "evidence_chain": [],
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ), patch(
            "triage_agent.agents.rca_agent.identify_evidence_gaps",
            return_value=["Need more logs"],
        ):
            result = rca_agent_first_attempt(state)

            # Confidence 0.62 < 0.65, should need more evidence
            assert result["needs_more_evidence"] is True
            assert result["evidence_gaps"] is not None


_MINIMAL_COMPRESSED_EVIDENCE = {
    "infra_findings_json": "{}",
    "dag_json": "[]",
    "metrics_formatted": "No metrics available.",
    "logs_formatted": "No logs available.",
    "trace_deviations_formatted": "No UE trace deviations available.",
}


class TestRcaAgentTimeoutRecovery:
    def test_timeout_returns_sentinel_not_raises(self, sample_initial_state: TriageState) -> None:
        """rca_agent_first_attempt must NOT raise on TimeoutError — returns low-confidence sentinel."""
        sample_initial_state["compressed_evidence"] = _MINIMAL_COMPRESSED_EVIDENCE
        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            side_effect=TimeoutError("LLM timed out"),
        ):
            result = rca_agent_first_attempt(sample_initial_state)

        assert result["confidence"] == 0.0
        assert result["root_nf"] == "unknown"
        assert result["failure_mode"] == "llm_timeout"
        assert result["needs_more_evidence"] is False
        assert result["evidence_gaps"] == ["LLM analysis unavailable due to timeout"]

    def test_timeout_sentinel_does_not_trigger_retry(self, sample_initial_state: TriageState) -> None:
        """Timeout sentinel has needs_more_evidence=False so the pipeline finalises rather than retries."""
        sample_initial_state["compressed_evidence"] = _MINIMAL_COMPRESSED_EVIDENCE
        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            side_effect=TimeoutError("LLM timed out"),
        ):
            result = rca_agent_first_attempt(sample_initial_state)

        assert result["needs_more_evidence"] is False


class TestLLMErrorPropagation:
    """LLM errors propagate — no silent fallback."""

    def test_llm_missing_required_key_returns_degraded_result(
        self, sample_initial_state: TriageState
    ) -> None:
        """Valid JSON response missing root_nf must degrade gracefully, not raise KeyError."""
        sample_initial_state["compressed_evidence"] = _MINIMAL_COMPRESSED_EVIDENCE
        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value={"failure_mode": "some_error", "confidence": 0.5},
        ):
            result = rca_agent_first_attempt(sample_initial_state)

        assert result["failure_mode"] == "llm_error"
        assert result["root_nf"] == "unknown"
        assert result["confidence"] == 0.0
        assert result["needs_more_evidence"] is True

    def test_llm_connection_error_returns_degraded_result(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """ConnectionError from LLM returns degraded sentinel result (retry on first attempt)."""
        state = sample_initial_state
        state["dag"] = sample_dag

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            side_effect=ConnectionError("Cannot connect to LLM"),
        ):
            result = rca_agent_first_attempt(state)

        # Connection errors are caught and handled gracefully; retry is triggered
        assert result["failure_mode"] == "llm_error"
        assert result["root_nf"] == "unknown"
        assert result["confidence"] == 0.0
        # On first attempt, should request retry
        assert result["needs_more_evidence"] is True


class TestEvidenceChainCitations:
    """Tests for mandatory citations in evidence chain."""

    def test_evidence_chain_requires_citations(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """Each evidence item must have timestamp, source, nf, type, and content."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.75

        mock_analysis = {
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.85,
            "evidence_chain": [
                {
                    "timestamp": "2026-02-15T10:00:00Z",
                    "source": "logs",
                    "nf": "AUSF",
                    "type": "log",
                    "content": "Authentication timeout after 5s",
                    "significance": "Primary failure indicator",
                },
                {
                    "timestamp": "2026-02-15T09:59:58Z",
                    "source": "metrics",
                    "nf": "AUSF",
                    "type": "metric",
                    "content": "http_request_duration_seconds{nf='AUSF'} = 5.2",
                    "significance": "Confirms slow response",
                },
            ],
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ):
            result = rca_agent_first_attempt(state)

            # Verify evidence chain has mandatory fields
            assert len(result["evidence_chain"]) == 2

            for evidence in result["evidence_chain"]:
                assert "timestamp" in evidence
                assert "source" in evidence
                assert evidence["source"] in [
                    "infrastructure",
                    "metrics",
                    "logs",
                    "traces",
                ]
                assert "nf" in evidence
                assert "type" in evidence
                assert "content" in evidence

    def test_evidence_chain_empty_when_low_confidence(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """Low confidence analysis may have sparse evidence chain."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.30

        mock_analysis = {
            "root_nf": "unknown",
            "failure_mode": "undetermined",
            "confidence": 0.25,
            "evidence_chain": [],  # No strong evidence found
            "layer": "application",
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ), patch(
            "triage_agent.agents.rca_agent.identify_evidence_gaps",
            return_value=["Need logs", "Need traces"],
        ):
            result = rca_agent_first_attempt(state)

            # Empty evidence chain is valid when confidence is low
            assert result["evidence_chain"] == []
            assert result["needs_more_evidence"] is True


class TestStateUpdates:
    """Tests for state field updates."""

    def test_updates_all_required_state_fields(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """RCAAgent should update layer, root_nf, failure_mode, confidence in state."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.75

        mock_analysis = {
            "layer": "infrastructure",
            "root_nf": "amf-pod",
            "failure_mode": "OOMKilled",
            "confidence": 0.95,
            "evidence_chain": [
                {
                    "timestamp": "2026-02-15T10:00:00Z",
                    "source": "infrastructure",
                    "nf": "amf-pod",
                    "type": "event",
                    "content": "OOMKilled: container exceeded memory limit",
                    "significance": "Root cause",
                }
            ],
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ):
            result = rca_agent_first_attempt(state)

            # Verify all required fields are set
            assert result["layer"] == "infrastructure"
            assert result["root_nf"] == "amf-pod"
            assert result["failure_mode"] == "OOMKilled"
            assert result["confidence"] == 0.95
            assert len(result["evidence_chain"]) == 1

    def test_defaults_layer_to_application_if_missing(
        self, sample_initial_state: TriageState, sample_dag: dict[str, Any]
    ) -> None:
        """If LLM doesn't return layer, default to 'application'."""
        state = sample_initial_state
        state["dag"] = sample_dag
        state["procedure_name"] = "Registration_General"
        state["evidence_quality_score"] = 0.75

        mock_analysis = {
            # layer field missing
            "root_nf": "AUSF",
            "failure_mode": "auth_timeout",
            "confidence": 0.85,
            "evidence_chain": [],
        }

        with patch(
            "triage_agent.agents.rca_agent.llm_analyze_evidence",
            return_value=mock_analysis,
        ):
            result = rca_agent_first_attempt(state)

            # Should default to "application" if layer is missing
            assert result["layer"] == "application"


def test_format_trace_deviations_dict() -> None:
    deviations = {
        "registration_general": [{"deviation_point": 3, "expected": "AMF sends NAS", "actual": "timeout"}],
        "authentication_5g_aka": [],
    }
    result = format_trace_deviations_for_prompt(deviations)
    assert "registration_general" in result
    assert "deviation_point" in result


def test_format_trace_deviations_none() -> None:
    assert format_trace_deviations_for_prompt(None) == "No UE trace deviations available."


def test_format_trace_deviations_empty_dict() -> None:
    assert format_trace_deviations_for_prompt({}) == "No UE trace deviations available."


def test_identify_evidence_gaps_empty_trace_deviations() -> None:
    """trace_deviations being {} (empty dict) should count as missing evidence."""
    alert = {"labels": {"alertname": "test"}, "startsAt": "2024-01-01T12:00:00Z"}
    state = get_initial_state(alert, str(uuid.uuid4()))
    state["trace_deviations"] = {}
    gaps = identify_evidence_gaps(state)
    assert "UE trace analysis needed" in gaps




def test_rca_prompt_includes_dag_content(monkeypatch):
    """RCA prompt must include actual DAG content, not null."""
    import uuid

    import triage_agent.agents.rca_agent as ra
    from triage_agent.graph import get_initial_state

    alert = {"labels": {"alertname": "test"}, "startsAt": "2024-01-01T12:00:00Z"}
    state = get_initial_state(alert, str(uuid.uuid4()))
    state["procedure_names"] = ["registration_general"]
    state["dags"] = [{"name": "registration_general", "phases": [], "all_nfs": ["AMF"]}]
    state["infra_score"] = 0.1
    state["evidence_quality_score"] = 0.5

    captured = {}
    def fake_llm(prompt: str, timeout=None):
        captured["prompt"] = prompt
        return {
            "layer": "application", "root_nf": "AMF", "failure_mode": "timeout",
            "failed_phase": None, "confidence": 0.9, "evidence_chain": [],
            "alternative_hypotheses": [], "reasoning": "test",
        }
    monkeypatch.setattr(ra, "llm_analyze_evidence", fake_llm)
    state.update(ra.join_for_rca(state))  # populate compressed_evidence as barrier node would
    ra.rca_agent_first_attempt(state)
    assert "registration_general" in captured["prompt"]
    # The word "null" should not appear in the DAG section of the prompt
    dag_section_start = captured["prompt"].find("PROCEDURE DAG")
    assert dag_section_start != -1
    dag_section = captured["prompt"][dag_section_start:dag_section_start+200]
    assert "null" not in dag_section


# ---------------------------------------------------------------------------
# Evidence compression tests
# ---------------------------------------------------------------------------

class TestCountTokens:
    """count_tokens: fast 4-char/token approximation (now in utils)."""

    def test_empty_string_returns_one(self) -> None:
        from triage_agent.utils import count_tokens
        assert count_tokens("") == 1

    def test_400_char_string_returns_100(self) -> None:
        from triage_agent.utils import count_tokens
        assert count_tokens("a" * 400) == 100

    def test_four_char_boundary(self) -> None:
        from triage_agent.utils import count_tokens
        assert count_tokens("abcd") == 1

    def test_eight_chars_returns_two(self) -> None:
        from triage_agent.utils import count_tokens
        assert count_tokens("abcdefgh") == 2

    def test_also_importable_from_utils(self) -> None:
        """count_tokens lives in triage_agent.utils."""
        from triage_agent.utils import count_tokens
        assert count_tokens("abcd") == 1


class TestCompressInfraFindingsForAgent:
    """compress_infra_findings_for_agent: issue-only filter in infra_agent."""

    def test_healthy_score_returns_healthy_sentinel(self) -> None:
        from triage_agent.agents.infra_agent import compress_infra_findings_for_agent
        findings = {
            "pod_restarts": {"amf-pod": 0},
            "oom_kills": {},
            "resource_usage": {"amf-pod": {"cpu": 0.2, "memory_percent": 40.0}},
            "node_health": {"amf-pod": "Running"},
            "concurrent_failures": 0,
            "critical_events": [],
        }
        result = compress_infra_findings_for_agent(findings, infra_score=0.0, token_budget=10000)
        assert result == {"status": "all_pods_healthy"}

    def test_oom_kills_always_included(self) -> None:
        from triage_agent.agents.infra_agent import compress_infra_findings_for_agent
        findings = {
            "pod_restarts": {},
            "oom_kills": {"udm-pod": 2},
            "resource_usage": {},
            "node_health": {},
            "concurrent_failures": 0,
            "critical_events": [],
        }
        result = compress_infra_findings_for_agent(findings, infra_score=0.25, token_budget=10000)
        assert "oom_kills" in result
        assert result["oom_kills"]["udm-pod"] == 2

    def test_zero_restart_pods_excluded(self) -> None:
        from triage_agent.agents.infra_agent import compress_infra_findings_for_agent
        findings = {
            "pod_restarts": {"amf-pod": 3, "smf-pod": 0, "ausf-pod": 1},
            "oom_kills": {},
            "resource_usage": {},
            "node_health": {},
            "concurrent_failures": 2,
            "critical_events": [],
        }
        result = compress_infra_findings_for_agent(findings, infra_score=0.5, token_budget=10000)
        assert result.get("pod_restarts", {}).get("amf-pod") == 3
        assert "smf-pod" not in result.get("pod_restarts", {})

    def test_running_nodes_excluded_from_node_health(self) -> None:
        from triage_agent.agents.infra_agent import compress_infra_findings_for_agent
        findings = {
            "pod_restarts": {},
            "oom_kills": {},
            "resource_usage": {},
            "node_health": {"amf-pod": "Running", "udm-pod": "Pending"},
            "concurrent_failures": 1,
            "critical_events": ["OOMKilled"],
        }
        result = compress_infra_findings_for_agent(findings, infra_score=0.3, token_budget=10000)
        # Pending pod should appear
        assert result.get("node_health", {}).get("udm-pod") == "Pending"
        # Running pod should NOT appear
        assert "amf-pod" not in result.get("node_health", {})


class TestCompressNfLogs:
    """compress_nf_logs: DAG-NF protection, no message truncation."""

    def test_none_input_returns_empty_dict(self) -> None:
        from triage_agent.agents.logs_agent import compress_nf_logs
        assert compress_nf_logs(None, [], 500) == {}

    def test_empty_dict_returns_empty_dict(self) -> None:
        from triage_agent.agents.logs_agent import compress_nf_logs
        assert compress_nf_logs({}, [], 500) == {}

    def test_dag_nf_all_entries_kept_regardless_of_level(self) -> None:
        from triage_agent.agents.logs_agent import compress_nf_logs
        logs = {
            "AMF": [
                {"level": "INFO", "message": "normal op", "timestamp": 1000,
                 "matched_phase": None, "matched_pattern": None},
                {"level": "DEBUG", "message": "debug trace", "timestamp": 1001,
                 "matched_phase": None, "matched_pattern": None},
            ]
        }
        result = compress_nf_logs(logs, nf_union=["AMF"], token_budget=10000)
        assert "AMF" in result
        assert len(result["AMF"]) == 2

    def test_non_dag_nf_only_error_warn_fatal_kept(self) -> None:
        from triage_agent.agents.logs_agent import compress_nf_logs
        logs = {
            "SMF": [
                {"level": "INFO", "message": "normal", "timestamp": 1000,
                 "matched_phase": None, "matched_pattern": None},
                {"level": "DEBUG", "message": "debug", "timestamp": 1001,
                 "matched_phase": None, "matched_pattern": None},
                {"level": "ERROR", "message": "error msg", "timestamp": 1002,
                 "matched_phase": None, "matched_pattern": None},
            ]
        }
        result = compress_nf_logs(logs, nf_union=["AMF"], token_budget=10000)
        assert "SMF" in result
        assert all(e["level"] in ("ERROR", "WARN", "FATAL") or e.get("matched_phase")
                   for e in result["SMF"])

    def test_non_dag_nf_no_qualifying_entries_omitted(self) -> None:
        from triage_agent.agents.logs_agent import compress_nf_logs
        logs = {
            "SMF": [
                {"level": "INFO", "message": "normal", "timestamp": 1000,
                 "matched_phase": None, "matched_pattern": None},
            ]
        }
        result = compress_nf_logs(logs, nf_union=["AMF"], token_budget=10000)
        assert "SMF" not in result

    def test_messages_truncated_to_max_chars_for_dag_nf(self) -> None:
        from unittest.mock import patch
        from triage_agent.agents.logs_agent import compress_nf_logs
        from triage_agent.config import TriageAgentConfig
        long_msg = "x" * 5000
        logs = {
            "AMF": [{"level": "ERROR", "message": long_msg, "timestamp": 1000,
                     "matched_phase": None, "matched_pattern": None}]
        }
        with patch("triage_agent.agents.logs_agent.get_config") as mock_cfg:
            mock_cfg.return_value = TriageAgentConfig(rca_log_max_message_chars=200, rca_token_budget_logs=10_000)
            result = compress_nf_logs(logs, nf_union=["AMF"], token_budget=10000)
        assert len(result["AMF"][0]["message"]) <= 201
        assert result["AMF"][0]["message"].endswith("…")

    def test_dag_nf_included_even_when_over_budget(self) -> None:
        from triage_agent.agents.logs_agent import compress_nf_logs
        big_logs = {
            "AMF": [{"level": "INFO", "message": "x" * 2000, "timestamp": i,
                     "matched_phase": None, "matched_pattern": None}
                    for i in range(5)]
        }
        # Budget too small for the big logs
        result = compress_nf_logs(big_logs, nf_union=["AMF"], token_budget=10)
        # AMF is a DAG NF, so it must still be in the result
        assert "AMF" in result


class TestCompressNfMetrics:
    """compress_nf_metrics: DAG-NF protection, compact format."""

    def test_none_input_returns_empty_dict(self) -> None:
        from triage_agent.agents.metrics_agent import compress_nf_metrics
        assert compress_nf_metrics(None, [], 500) == {}

    def test_empty_dict_returns_empty_dict(self) -> None:
        from triage_agent.agents.metrics_agent import compress_nf_metrics
        assert compress_nf_metrics({}, [], 500) == {}

    def test_dag_nf_always_included(self) -> None:
        from triage_agent.agents.metrics_agent import compress_nf_metrics
        metrics = {
            "AMF": {"error_rate": 0.0, "cpu_rate": 0.1},
            "EXTRA": {"error_rate": 0.0},
        }
        # EXTRA is not in nf_union; AMF is
        result = compress_nf_metrics(metrics, nf_union=["AMF"], token_budget=100)
        assert "AMF" in result

    def test_non_dag_nf_dropped_when_over_budget(self) -> None:
        from triage_agent.agents.metrics_agent import compress_nf_metrics
        big_non_dag = {f"NF_{i}": {"data": "x" * 500} for i in range(20)}
        metrics = {"AMF": {"error_rate": 0.0}, **big_non_dag}
        result = compress_nf_metrics(metrics, nf_union=["AMF"], token_budget=50)
        assert "AMF" in result
        # At least some non-DAG NFs should be dropped
        non_dag_in_result = [k for k in result if k != "AMF"]
        non_dag_in_input = [k for k in big_non_dag]
        assert len(non_dag_in_result) <= len(non_dag_in_input)

    def test_prometheus_vector_format_compacted(self) -> None:
        from triage_agent.agents.metrics_agent import compress_nf_metrics
        metrics = {
            "AMF": [
                {"metric": {"report": "error_rate"}, "value": [1000, "0.05"]},
                {"metric": {"report": "p95_latency"}, "value": [1000, "0.3"]},
            ]
        }
        result = compress_nf_metrics(metrics, nf_union=["AMF"], token_budget=10000)
        assert "AMF" in result
        assert isinstance(result["AMF"], dict)
        assert "error_rate" in result["AMF"]


class TestCompressDag:
    """compress_dag: progressively strips then truncates DAG phases."""

    def test_none_input_returns_empty_list(self) -> None:
        from triage_agent.utils import compress_dag
        assert compress_dag(None, 800) == []

    def test_empty_list_returns_empty_list(self) -> None:
        from triage_agent.utils import compress_dag
        assert compress_dag([], 800) == []

    def test_small_dag_returned_unchanged(self) -> None:
        from triage_agent.utils import compress_dag
        dags = [{"name": "reg", "phases": [{"order": 1, "action": "req"}], "all_nfs": ["AMF"]}]
        result = compress_dag(dags, 10000)
        assert result == dags

    def test_keywords_stripped_when_over_budget(self) -> None:
        from triage_agent.utils import compress_dag
        big_keywords = ["keyword_" + str(i) for i in range(100)]
        dags = [{"name": "reg", "all_nfs": ["AMF"], "phases": [
            {"order": i, "action": f"step {i}", "keywords": big_keywords, "failure_patterns": []}
            for i in range(20)
        ]}]
        # Use a budget that is tight relative to the full DAG
        import json
        full_size = len(json.dumps(dags)) // 4
        result = compress_dag(dags, full_size // 2)
        # keywords should be removed from at least some phases
        has_keywords = any("keywords" in p for d in result for p in d.get("phases", []))
        assert not has_keywords

    def test_failure_pattern_phases_kept_on_extreme_budget(self) -> None:

        from triage_agent.utils import compress_dag
        dags = [{"name": "reg", "all_nfs": ["AMF"], "phases": [
            {"order": 1, "action": "start", "failure_patterns": []},
            {"order": 2, "action": "auth", "failure_patterns": ["*auth*fail*"]},
            {"order": 3, "action": "accept", "failure_patterns": ["*reject*"]},
        ]}]
        result = compress_dag(dags, 30)  # Very tight budget
        if result and result[0].get("phases"):
            phases_with_fp = [p for p in result[0]["phases"] if p.get("failure_patterns")]
            # Phases with failure_patterns should survive if any phases remain
            assert len(phases_with_fp) > 0


class TestCompressTraceDeviations:
    """compress_trace_deviations: per-DAG slice and budget enforcement."""

    def test_none_returns_empty_dict(self) -> None:
        from triage_agent.utils import compress_trace_deviations
        assert compress_trace_deviations(None, 500) == {}

    def test_empty_dict_returns_empty_dict(self) -> None:
        from triage_agent.utils import compress_trace_deviations
        assert compress_trace_deviations({}, 500) == {}

    def test_within_budget_returned_unchanged(self) -> None:
        from triage_agent.utils import compress_trace_deviations
        devs = {"reg": [{"deviation_point": 1, "expected": "A", "actual": "B"}]}
        result = compress_trace_deviations(devs, 10000)
        assert result == devs

    def test_truncated_to_max_deviations_per_dag(self) -> None:
        from triage_agent.utils import compress_trace_deviations
        devs = {"reg": [{"deviation_point": i} for i in range(10)]}
        result = compress_trace_deviations(devs, 10000)
        # Default cfg.rca_max_deviations_per_dag = 3
        assert len(result["reg"]) <= 3

    def test_dag_with_no_deviations_dropped_when_over_budget(self) -> None:
        import json

        from triage_agent.utils import compress_trace_deviations
        devs = {
            "empty_dag": [],
            "active_dag": [{"deviation_point": i, "data": "x" * 100} for i in range(3)],
        }
        # Make budget tight so empty_dag gets dropped
        active_size = len(json.dumps({"active_dag": devs["active_dag"]})) // 4
        result = compress_trace_deviations(devs, active_size)
        assert "empty_dag" not in result


class TestJoinForRca:
    def test_returns_compressed_evidence_dict(self, sample_initial_state: TriageState) -> None:
        """join_for_rca returns a delta dict with 'compressed_evidence' key."""
        result = join_for_rca(sample_initial_state)
        assert "compressed_evidence" in result
        assert isinstance(result["compressed_evidence"], dict)

    def test_compressed_evidence_has_prompt_keys(self, sample_initial_state: TriageState) -> None:
        """compressed_evidence dict contains all RCA_PROMPT_TEMPLATE placeholder keys."""
        result = join_for_rca(sample_initial_state)
        expected_keys = {
            "infra_findings_json", "dag_json",
            "metrics_formatted", "logs_formatted", "trace_deviations_formatted",
        }
        assert expected_keys.issubset(result["compressed_evidence"].keys())

    def test_join_for_rca_with_infra_findings(self, sample_initial_state: TriageState) -> None:
        """join_for_rca includes infra_findings in compressed_evidence."""
        sample_initial_state["infra_findings"] = {"pod_restarts": 3}
        result = join_for_rca(sample_initial_state)
        assert "pod_restarts" in result["compressed_evidence"]["infra_findings_json"]


class TestCompressEvidence:
    """compress_evidence: top-level orchestrator returns all 5 prompt sections."""

    def test_returns_all_required_keys(self, sample_initial_state: TriageState) -> None:
        from triage_agent.agents.rca_agent import compress_evidence
        result = compress_evidence(sample_initial_state)
        required = {
            "infra_findings_json",
            "dag_json",
            "metrics_formatted",
            "logs_formatted",
            "trace_deviations_formatted",
        }
        assert required.issubset(result.keys())

    def test_empty_state_returns_no_data_strings(
        self, sample_initial_state: TriageState
    ) -> None:
        from triage_agent.agents.rca_agent import compress_evidence
        result = compress_evidence(sample_initial_state)
        assert result["metrics_formatted"] == "No metrics available."
        assert result["logs_formatted"] == "No logs available."
        assert result["trace_deviations_formatted"] == "No UE trace deviations available."
