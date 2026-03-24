"""RCAAgent: Root cause analysis using LLM.

The only agent that uses an LLM. Receives infrastructure findings,
NF metrics, NF logs, UE trace deviations, and DAG structure.
Produces root_nf, failure_mode, confidence, evidence_chain.
"""

import json
import logging
import time
from typing import Any, Literal

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langsmith import traceable
from pydantic import BaseModel, Field, SecretStr

from triage_agent.config import get_config
from triage_agent.state import TriageState
from triage_agent.utils import compress_dag, compress_trace_deviations

logger = logging.getLogger(__name__)

# --- Pydantic Models ---


class EvidenceItem(BaseModel):
    """Single piece of evidence in the chain."""

    timestamp: str
    source: Literal["infrastructure", "metrics", "logs", "traces"]
    nf: str
    type: str  # log, metric, event, trace_deviation
    content: str
    significance: str


class RCAOutput(BaseModel):
    """Structured output from RCA LLM analysis."""

    layer: Literal["infrastructure", "application"]
    root_nf: str
    failure_mode: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_chain: list[EvidenceItem]


# --- LLM Prompt Template ---

RCA_PROMPT_TEMPLATE = """\
You are a 5G network expert performing root cause analysis for a {procedure_name} failure.

INFRASTRUCTURE FINDINGS (from InfraAgent):
Infrastructure Score: {infra_score} (0.0 = no infra issue, 1.0 = confirmed infra issue)
{infra_findings_json}

PROCEDURE DAGs (reference procedures for this alert):
{dag_json}

APPLICATION EVIDENCE (time window: {time_window}):

METRICS:
{metrics_formatted}

LOGS (annotated with matched DAG phases):
{logs_formatted}

UE TRACE DEVIATIONS (from Memgraph comparison against reference DAG):
{trace_deviations_formatted}

EVIDENCE QUALITY: {evidence_quality_score}

ANALYSIS FRAMEWORK:
1. Layer Determination:
   - If infra_score >= {infra_root_cause_threshold}: Likely infrastructure root cause
   - If infra_score >= {infra_triggered_threshold}: Possible infrastructure-triggered application failure
   - If infra_score < {app_only_threshold}: Likely pure application failure

2. Root Cause Identification:
   - Use temporal precedence (earliest anomaly in time window)
   - Use DAG topology (upstream NFs more likely to be root cause)
   - Correlate infrastructure findings with application symptoms
   - Match log messages against DAG failure_patterns (wildcard matching)

3. Infrastructure vs Application Decision:
   - Infrastructure root cause: Pod-level issues (OOMKill, CrashLoop, resource exhaustion)
   - Application root cause: NF logic errors, protocol failures, data validation errors
   - Infrastructure-triggered application: Infra issue causes cascading app failures

CONFIDENCE FORMULA — compute confidence mechanically before writing the JSON:
  start = 0.50
  +0.20 if the same error pattern appears in 3 or more log entries from the root_nf
  +0.20 if trace_deviations exist and confirm the same root_nf deviates from the DAG
  +0.10 if evidence_quality_score >= 0.90
  +0.05 if infra_score == 0 and root cause is clearly application-layer
  -0.20 if evidence comes from only one source and fewer than 3 entries total
  confidence = max(0.0, min(1.0, start + adjustments))

Example calculations:
- infra_score=0.0, 6× "AmfUe is nil" from AMF, trace deviations confirm AMF, quality=0.95
  → 0.50 +0.20 +0.20 +0.10 +0.05 = 1.00  → confidence=1.00
- infra_score=0.90, OOMKill only, no app errors
  → 0.50 +0.20 +0.05 = 0.75  → confidence=0.75
- infra_score=0.0, single auth-timeout log, no traces
  → 0.50 -0.20 = 0.30  → confidence=0.30

Return ONLY a JSON object with no markdown or extra text.
Include 2-4 evidence_chain items maximum (the most significant ones only).
Keep each "significance" value to ≤ 20 words.
List evidence_chain first, then apply the CONFIDENCE FORMULA to set confidence:
{{
  "layer": "infrastructure|application",
  "root_nf": "<NF name or 'pod-level' for infrastructure>",
  "failure_mode": "<concise description, ≤ 15 words>",
  "evidence_chain": [
    {{
      "timestamp": "<ISO timestamp>",
      "source": "infrastructure|metrics|logs|traces",
      "nf": "<NF name>",
      "type": "log|metric|event|trace_deviation",
      "content": "<brief excerpt, ≤ 20 words>",
      "significance": "<≤ 20 words: why this implicates root_nf>"
    }}
  ],
  "confidence": <apply CONFIDENCE FORMULA after listing evidence_chain above>
}}
"""


def format_metrics_for_prompt(metrics: dict[str, Any] | None) -> str:
    if not metrics:
        return "No metrics available."
    return json.dumps(metrics, separators=(",", ":"))


def format_logs_for_prompt(logs: dict[str, Any] | None) -> str:
    if not logs:
        return "No logs available."
    return json.dumps(logs, separators=(",", ":"))


def format_trace_deviations_for_prompt(deviations: dict[str, list[dict[str, Any]]] | None) -> str:
    if not deviations:
        return "No UE trace deviations available."
    return json.dumps(deviations, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Evidence compression — DAG and trace deviations (per-agent for infra/metrics/logs)
# ---------------------------------------------------------------------------
#
# count_tokens, compress_nf_metrics, compress_nf_logs live in their respective
# agents (metrics_agent.py, logs_agent.py) and in utils.py.  By the time
# compress_evidence() is called, state["infra_findings"], state["metrics"], and
# state["logs"] are already pre-compressed.  Only DAGs and trace deviations are
# compressed here (they have no dedicated agent-level compression step).



def compress_evidence(state: "TriageState") -> dict[str, str]:
    """Format evidence sections for the LLM prompt.

    infra_findings, metrics, and logs are already compressed by their respective
    agents before being written to state.  Only DAGs and trace_deviations are
    compressed here (they have no dedicated agent-level step).

    Returns a dict keyed by RCA_PROMPT_TEMPLATE placeholders.
    """
    cfg = get_config()
    compressed_dags = compress_dag(state.get("dags"), cfg.rca_token_budget_dag)
    compressed_traces = compress_trace_deviations(
        state.get("trace_deviations"), cfg.rca_token_budget_traces
    )
    return {
        "infra_findings_json": json.dumps(state.get("infra_findings") or {}, separators=(",", ":")),
        "dag_json": json.dumps(compressed_dags, separators=(",", ":")),
        "metrics_formatted": format_metrics_for_prompt(state.get("metrics")),
        "logs_formatted": format_logs_for_prompt(state.get("logs")),
        "trace_deviations_formatted": format_trace_deviations_for_prompt(compressed_traces),
    }


@traceable(name="join_for_rca")
def join_for_rca(state: TriageState) -> dict[str, Any]:
    """Barrier node: waits for infra_agent + evidence_quality, then compresses all evidence.

    This is the explicit synchronisation point that guarantees infra_agent data
    is present in state before the LLM prompt is built. It replaces the previous
    implicit superstep-merge assumption.
    """
    compressed = compress_evidence(state)
    return {"compressed_evidence": compressed}


def create_llm(
    provider: str,
    model: str,
    api_key: str,
    timeout: int,
    base_url: str = "",
) -> Any:
    """Factory: construct the appropriate LangChain chat model.

    Args:
        provider: One of "openai", "anthropic", "local"
        model: Model name string (provider-specific)
        api_key: API key; empty string allowed for local provider
        timeout: Request timeout in seconds
        base_url: Only used for "local" provider

    Returns:
        A LangChain chat model with .invoke() method

    Raises:
        ImportError: If provider == "anthropic" and langchain-anthropic is not installed
        ValueError: If provider == "local" and base_url is empty, or unknown provider
    """
    temperature = get_config().llm_temperature
    if provider == "openai":
        return ChatOpenAI(
            model=model,
            api_key=SecretStr(api_key) if api_key else None,
            temperature=temperature,
            timeout=timeout,
            model_kwargs={"max_tokens": get_config().llm_max_tokens},
            streaming=True,
        )
    elif provider == "anthropic":
        try:
            from langchain_anthropic import ChatAnthropic  # type: ignore[import-not-found]
        except ImportError as e:
            raise ImportError(
                "langchain-anthropic is required for the 'anthropic' provider. "
                "Install it with: pip install triage-agent[anthropic]"
            ) from e
        return ChatAnthropic(
            model=model,
            api_key=SecretStr(api_key) if api_key else None,
            temperature=temperature,
            timeout=timeout,
            max_tokens=get_config().llm_max_tokens,
            streaming=True,
        )
    elif provider == "local":
        if not base_url:
            raise ValueError(
                "llm_base_url must be set when llm_provider is 'local'. "
                "Set LLM_BASE_URL env var to the OpenAI-compatible endpoint, "
                "e.g. http://vllm-service.5g-core:8080/v1"
            )
        return ChatOpenAI(
            model=model,
            api_key=SecretStr(api_key) if api_key else SecretStr("local"),
            base_url=base_url,
            temperature=temperature,
            timeout=timeout,
            model_kwargs={"max_tokens": get_config().llm_max_tokens, "response_format": {"type": "json_object"}},
            streaming=True,
        )
    else:
        raise ValueError(f"Unsupported llm_provider: '{provider}'")


def llm_analyze_evidence(prompt: str, timeout: int | None = None) -> dict[str, Any]:
    """Call LLM with the RCA prompt. Returns parsed JSON response.

    Args:
        prompt: The formatted RCA prompt
        timeout: Optional timeout in seconds (defaults to config.llm_timeout)

    Returns:
        Parsed JSON dict from LLM response

    Raises:
        TimeoutError: If LLM request exceeds timeout
        ValueError: If LLM response is not valid JSON
    """
    config = get_config()
    timeout_val = timeout or config.llm_timeout

    # Initialize LLM client via factory (supports openai / anthropic / local)
    llm = create_llm(
        provider=config.llm_provider,
        model=config.llm_model,
        api_key=config.llm_api_key,
        timeout=timeout_val,
        base_url=config.llm_base_url,
    )

    messages = [
        SystemMessage(
            content="You are a 5G network expert performing root cause analysis. "
            "Always respond with valid JSON only, no markdown formatting."
        ),
        HumanMessage(content=prompt),
    ]

    try:
        response = llm.invoke(messages)

        # Handle response content which might be str or list
        if isinstance(response.content, str):
            response_text = response.content.strip()
        else:
            # If it's a list, join it
            response_text = "".join(str(item) for item in response.content).strip()

        # Remove markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        response_text = response_text.strip()

        # Parse JSON
        analysis: dict[str, Any] = json.loads(response_text)
        return analysis

    except Exception as e:
        # Convert any timeout-related exception to TimeoutError
        if "timeout" in str(e).lower() or "timed out" in str(e).lower():
            raise TimeoutError(f"LLM request timed out after {timeout_val}s") from e
        raise



def identify_evidence_gaps(state: TriageState) -> list[str]:
    """Identify what additional evidence is needed for higher confidence.

    Args:
        state: Current triage state

    Returns:
        List of evidence gap descriptions
    """
    gaps = []

    # Check for missing data sources
    if not state.get("metrics") or state.get("metrics") == {}:
        gaps.append("NF metrics data needed")

    if not state.get("logs") or state.get("logs") == {}:
        gaps.append("NF logs data needed")

    if not state.get("trace_deviations"):
        gaps.append("UE trace analysis needed")

    cfg = get_config()
    # Check evidence quality
    if state.get("evidence_quality_score", 0.0) < cfg.evidence_gap_quality_threshold:
        gaps.append("Overall evidence quality too low")

    # Check infrastructure findings
    if state.get("infra_score", 0.0) > cfg.infra_triggered_threshold and not state.get("infra_findings"):
        gaps.append("Detailed infrastructure findings needed")

    # If no specific gaps identified but confidence is low
    if not gaps and state.get("confidence", 0.0) < cfg.evidence_gap_confidence_threshold:
        gaps.append("Additional temporal analysis needed")
        gaps.append("Cross-correlation of events needed")

    return gaps


@traceable(name="rca_agent_first_attempt")
def rca_agent_first_attempt(state: TriageState) -> dict[str, Any]:
    """RCAAgent first attempt. Uses LLM for analysis.

    Evidence is compressed to fit within the LLM's context window before
    the prompt is built.

    Args:
        state: Current triage state with all evidence collected

    Returns:
        Delta dict with RCA results for LangGraph state merge
    """
    _cfg = get_config()
    # compressed_evidence is always present — populated by join_for_rca barrier node.
    # A KeyError here means the graph topology is broken (rca_agent reachable without join_for_rca).
    evidence = state["compressed_evidence"]
    if evidence is None:
        raise RuntimeError(
            "compressed_evidence is None — join_for_rca barrier node must run before rca_agent_first_attempt"
        )
    prompt = RCA_PROMPT_TEMPLATE.format(
        procedure_name=", ".join(state.get("procedure_names") or ["unknown"]),
        infra_score=state.get("infra_score", 0.0),
        time_window="alert_time - 5min to alert_time + 60s",
        evidence_quality_score=state.get("evidence_quality_score", 0.0),
        infra_root_cause_threshold=_cfg.infra_root_cause_threshold,
        infra_triggered_threshold=_cfg.infra_triggered_threshold,
        app_only_threshold=_cfg.app_only_threshold,
        **evidence,
    )

    try:
        analysis = llm_analyze_evidence(prompt)
    except TimeoutError:
        logger.warning(
            "LLM timed out for incident %s; returning low-confidence sentinel",
            state.get("incident_id"),
        )
        return {
            "root_nf": "unknown",
            "failure_mode": "llm_timeout",
            "confidence": 0.0,
            "evidence_chain": [],
            "layer": "unknown",
            "needs_more_evidence": False,
            "evidence_gaps": ["LLM analysis unavailable due to timeout"],
        }
    except Exception as e:
        # Malformed JSON or other unexpected LLM output error (including 503 "Loading model").
        # Retry if we have attempts remaining; otherwise degrade gracefully.
        attempt = state.get("attempt_count", 1)
        cfg = get_config()
        will_retry = attempt < cfg.max_attempts
        logger.warning(
            "LLM returned invalid output for incident %s (attempt %d, retry=%s): %s",
            state.get("incident_id"),
            attempt,
            will_retry,
            e,
        )
        # For 503 "Loading model" errors, back off before retry so the model
        # has time to finish loading (llama.cpp cold-start after long inference).
        if will_retry and ("503" in str(e) or "loading model" in str(e).lower()):
            logger.info(
                "LLM returned 503 Loading model for incident %s — sleeping 60s before retry",
                state.get("incident_id"),
            )
            time.sleep(60)
        return {
            "root_nf": "unknown",
            "failure_mode": "llm_error",
            "confidence": 0.0,
            "evidence_chain": [],
            "layer": "unknown",
            "needs_more_evidence": will_retry,
            "evidence_gaps": [f"LLM output error: {type(e).__name__}"],
        }

    try:
        root_nf = analysis["root_nf"]
        failure_mode = analysis["failure_mode"]
        confidence = analysis["confidence"]
    except KeyError as e:
        attempt = state.get("attempt_count", 1)
        cfg_ke = get_config()
        will_retry = attempt < cfg_ke.max_attempts
        logger.warning(
            "LLM response missing required key for incident %s (attempt %d, retry=%s): %s",
            state.get("incident_id"),
            attempt,
            will_retry,
            e,
        )
        return {
            "root_nf": "unknown",
            "failure_mode": "llm_error",
            "confidence": 0.0,
            "evidence_chain": [],
            "layer": "unknown",
            "needs_more_evidence": will_retry,
            "evidence_gaps": [f"LLM output missing required key: {e}"],
        }
    evidence_chain = analysis.get("evidence_chain", [])
    layer = analysis.get("layer", "application")

    # Deterministic override: if infra_score is above the root-cause threshold,
    # force layer=infrastructure regardless of LLM determination. The LLM may
    # misclassify when log evidence is present (e.g. "SBI server stopped" appears
    # as application evidence but is caused by the pod being absent — an infra event).
    _cfg_override = get_config()
    if state.get("infra_score", 0.0) >= _cfg_override.infra_root_cause_threshold:
        if layer != "infrastructure":
            logger.info(
                "Overriding LLM layer=%s → infrastructure (infra_score=%.2f >= threshold=%.2f)",
                layer,
                state.get("infra_score", 0.0),
                _cfg_override.infra_root_cause_threshold,
            )
        layer = "infrastructure"

    # Decision logic: determine if more evidence is needed
    cfg = get_config()
    min_confidence = cfg.min_confidence_default
    if state.get("evidence_quality_score", 0.0) >= cfg.high_evidence_threshold:
        min_confidence = cfg.min_confidence_relaxed

    if confidence >= min_confidence:
        needs_more_evidence = False
        evidence_gaps = None
    else:
        needs_more_evidence = True
        evidence_gaps = identify_evidence_gaps(state)

    return {
        "root_nf": root_nf,
        "failure_mode": failure_mode,
        "confidence": confidence,
        "evidence_chain": evidence_chain,
        "layer": layer,
        "needs_more_evidence": needs_more_evidence,
        "evidence_gaps": evidence_gaps,
    }
