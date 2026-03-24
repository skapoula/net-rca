"""Tests for configuration module.

Test-first: these tests define the expected behavior of TriageAgentConfig
and get_config() before implementation. Every test isolates from environment
variables using patch.dict(clear=True) to prevent BaseSettings env leakage.
"""

import os
from unittest.mock import patch

import pytest

from triage_agent.config import TriageAgentConfig, get_config

# Keys that BaseSettings might read from the environment.
# We clear these so host-level env vars don't leak into default-value tests.
_CONFIG_ENV_KEYS = [
    # infra_config
    "MEMGRAPH_HOST",
    "MEMGRAPH_PORT",
    "MEMGRAPH_POOL_SIZE",
    "MEMGRAPH_MAX_RETRIES",
    "KNOWN_NFS",
    "CORE_NAMESPACE",
    # api_config
    "PROMETHEUS_URL",
    "LOKI_URL",
    "MCP_TIMEOUT",
    "PROMETHEUS_MAX_RETRIES",
    "CORS_ALLOW_ORIGINS",
    "SERVER_HOST",
    "SERVER_PORT",
    # model_config
    "LLM_API_KEY",
    "LLM_MODEL",
    "LLM_TIMEOUT",
    "LLM_PROVIDER",
    "LLM_BASE_URL",
    "LLM_TEMPERATURE",
    # agent_config
    "MAX_ATTEMPTS",
    "MIN_CONFIDENCE_DEFAULT",
    "MIN_CONFIDENCE_RELAXED",
    "HIGH_EVIDENCE_THRESHOLD",
    "ALERT_LOOKBACK_SECONDS",
    "ALERT_LOOKAHEAD_SECONDS",
    "IMSI_DISCOVERY_WINDOW_SECONDS",
    "IMSI_TRACE_LOOKBACK_SECONDS",
    "IMSI_DIGIT_LENGTH",
    # query_config
    "PROMQL_RESTART_WINDOW",
    "PROMQL_OOM_WINDOW",
    "PROMQL_CPU_RATE_WINDOW_INFRA",
    "PROMQL_ERROR_RATE_WINDOW",
    "PROMQL_LATENCY_QUANTILE",
    "PROMQL_CPU_RATE_WINDOW_NF",
    "PROMQL_RANGE_STEP",
    "LOKI_QUERY_LIMIT",
    # scoring_config
    "INFRA_WEIGHT_RESTARTS",
    "INFRA_WEIGHT_OOM",
    "INFRA_WEIGHT_POD_STATUS",
    "INFRA_WEIGHT_RESOURCES",
    "RESTART_THRESHOLD_CRITICAL",
    "RESTART_THRESHOLD_HIGH",
    "RESTART_FACTOR_HIGH",
    "RESTART_FACTOR_LOW",
    "MEMORY_SATURATION_PCT",
    "CPU_SATURATION_CORES",
    "CPU_SATURATION_FACTOR",
    "POD_PENDING_FACTOR",
    "INFRA_ROOT_CAUSE_THRESHOLD",
    "INFRA_TRIGGERED_THRESHOLD",
    "APP_ONLY_THRESHOLD",
    "DEGRADED_CONF_INFRA_GENERIC",
    "DEGRADED_CONF_INFRA_SPECIFIC",
    "DEGRADED_CONF_APP_UNKNOWN",
    "DEGRADED_CONF_APP_PATTERN_MATCH",
    "EVIDENCE_GAP_QUALITY_THRESHOLD",
    "EVIDENCE_GAP_CONFIDENCE_THRESHOLD",
    "EQ_SCORE_ALL_SOURCES",
    "EQ_SCORE_TRACES_PLUS_ONE",
    "EQ_SCORE_METRICS_LOGS",
    "EQ_SCORE_TRACES_ONLY",
    "EQ_SCORE_METRICS_ONLY",
    "EQ_SCORE_LOGS_ONLY",
    "EQ_SCORE_NO_EVIDENCE",
    # observability_config
    "LANGSMITH_PROJECT",
    "LANGSMITH_API_KEY",
    "LANGCHAIN_TRACING_V2",
    "LANGCHAIN_PROJECT",
    "LANGCHAIN_ENDPOINT",
    "APP_VERSION",
]

_CLEAN_ENV = {k: v for k, v in os.environ.items() if k not in _CONFIG_ENV_KEYS}


class TestDefaultValues:
    """Default values are correct when no env vars are set."""

    def test_memgraph_defaults(self) -> None:
        """Memgraph host/port should default to localhost:7687."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")

        assert config.memgraph_host == "localhost"
        assert config.memgraph_port == 7687

    def test_mcp_url_defaults(self) -> None:
        """Prometheus and Loki URLs should default to cluster-local addresses."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")

        assert config.prometheus_url == "http://kube-prom-kube-prometheus-prometheus.monitoring:9090"
        assert config.loki_url == "http://loki.monitoring:3100"

    def test_mcp_timeout_default(self) -> None:
        """MCP timeout should default to 3.0 seconds."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")

        assert config.mcp_timeout == 3.0

    def test_llm_defaults(self) -> None:
        """LLM model and timeout should have sensible defaults."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")

        assert config.llm_model == "qwen3-4b-instruct-2507.Q4_K_M.gguf"
        assert config.llm_timeout == 300

    def test_langsmith_default_project(self) -> None:
        """LangSmith project should default to '5g-triage-agent'."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")

        assert config.langsmith_project == "5g-triage-agent"

    def test_llm_api_key_stored(self) -> None:
        """Explicitly passed llm_api_key should be stored."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="sk-test-123")

        assert config.llm_api_key == "sk-test-123"


class TestEnvironmentVariableOverride:
    """Environment variable override works for all fields."""

    def test_memgraph_host_from_env(self) -> None:
        """MEMGRAPH_HOST env var should override default."""
        with patch.dict(
            os.environ, {**_CLEAN_ENV, "MEMGRAPH_HOST": "mg.internal"}, clear=True
        ):
            config = TriageAgentConfig()

        assert config.memgraph_host == "mg.internal"

    def test_memgraph_port_from_env(self) -> None:
        """MEMGRAPH_PORT env var should override default and parse as int."""
        with patch.dict(
            os.environ, {**_CLEAN_ENV, "MEMGRAPH_PORT": "7700"}, clear=True
        ):
            config = TriageAgentConfig()

        assert config.memgraph_port == 7700

    def test_llm_api_key_from_env(self) -> None:
        """LLM_API_KEY env var should override default."""
        with patch.dict(
            os.environ, {**_CLEAN_ENV, "LLM_API_KEY": "env-api-key"}, clear=True
        ):
            config = TriageAgentConfig()

        assert config.llm_api_key == "env-api-key"

    def test_prometheus_url_from_env(self) -> None:
        """PROMETHEUS_URL env var should override default."""
        with patch.dict(
            os.environ,
            {**_CLEAN_ENV, "PROMETHEUS_URL": "http://custom-prom:9090"},
            clear=True,
        ):
            config = TriageAgentConfig()

        assert config.prometheus_url == "http://custom-prom:9090"

    def test_multiple_overrides(self) -> None:
        """Multiple env vars should all take effect simultaneously."""
        with patch.dict(
            os.environ,
            {
                **_CLEAN_ENV,
                "MEMGRAPH_HOST": "remote-mg",
                "MEMGRAPH_PORT": "7700",
                "LLM_API_KEY": "env-key",
                "PROMETHEUS_URL": "http://prom2:9090",
                "LOKI_URL": "http://loki2:3100",
            },
            clear=True,
        ):
            config = TriageAgentConfig()

        assert config.memgraph_host == "remote-mg"
        assert config.memgraph_port == 7700
        assert config.llm_api_key == "env-key"
        assert config.prometheus_url == "http://prom2:9090"
        assert config.loki_url == "http://loki2:3100"


class TestInvalidPortRaisesValueError:
    """Invalid port raises ValueError with descriptive message."""

    def test_negative_port(self) -> None:
        """Negative port should raise ValueError."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            with pytest.raises(ValueError, match="must be positive"):
                TriageAgentConfig(llm_api_key="test-key", memgraph_port=-1)

    def test_zero_port(self) -> None:
        """Zero port should raise ValueError."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            with pytest.raises(ValueError, match="must be positive"):
                TriageAgentConfig(llm_api_key="test-key", memgraph_port=0)

    def test_valid_port_does_not_raise(self) -> None:
        """Positive port should not raise."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key", memgraph_port=1234)

        assert config.memgraph_port == 1234


class TestInvalidUrlRaisesValueError:
    """Invalid URL raises ValueError with descriptive message."""

    def test_prometheus_url_missing_scheme(self) -> None:
        """prometheus_url without http:// should raise ValueError."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            with pytest.raises(ValueError, match="must start with http"):
                TriageAgentConfig(
                    llm_api_key="test-key",
                    prometheus_url="prometheus:9090",
                )

    def test_loki_url_missing_scheme(self) -> None:
        """loki_url without http:// should raise ValueError."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            with pytest.raises(ValueError, match="must start with http"):
                TriageAgentConfig(
                    llm_api_key="test-key",
                    loki_url="loki:3100",
                )

    def test_https_url_is_valid(self) -> None:
        """https:// URLs should pass validation."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(
                llm_api_key="test-key",
                prometheus_url="https://prom.example.com:9090",
            )

        assert config.prometheus_url == "https://prom.example.com:9090"

    def test_ftp_url_is_invalid(self) -> None:
        """ftp:// URLs should fail validation."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            with pytest.raises(ValueError, match="must start with http"):
                TriageAgentConfig(
                    llm_api_key="test-key",
                    prometheus_url="ftp://prometheus:9090",
                )


class TestMemgraphUriProperty:
    """memgraph_uri property computed correctly from host and port."""

    def test_default_uri(self) -> None:
        """Default host/port should produce bolt://localhost:7687."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")

        assert config.memgraph_uri == "bolt://localhost:7687"

    def test_custom_host_and_port(self) -> None:
        """Custom host/port should be reflected in URI."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(
                llm_api_key="test-key",
                memgraph_host="memgraph-server",
                memgraph_port=7688,
            )

        assert config.memgraph_uri == "bolt://memgraph-server:7688"

    def test_uri_from_env_override(self) -> None:
        """URI should reflect env var overrides."""
        with patch.dict(
            os.environ,
            {**_CLEAN_ENV, "MEMGRAPH_HOST": "mg-prod", "MEMGRAPH_PORT": "17687"},
            clear=True,
        ):
            config = TriageAgentConfig()

        assert config.memgraph_uri == "bolt://mg-prod:17687"


class TestLLMProviderConfig:
    """Tests for llm_provider and llm_base_url fields."""

    def test_llm_provider_defaults_to_local(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.llm_provider == "local"

    def test_llm_provider_from_env_anthropic(self) -> None:
        with patch.dict(os.environ, {**_CLEAN_ENV, "LLM_PROVIDER": "anthropic"}, clear=True):
            config = TriageAgentConfig()
        assert config.llm_provider == "anthropic"

    def test_llm_provider_from_env_local(self) -> None:
        with patch.dict(os.environ, {**_CLEAN_ENV, "LLM_PROVIDER": "local"}, clear=True):
            config = TriageAgentConfig()
        assert config.llm_provider == "local"

    def test_llm_provider_invalid_raises(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True), pytest.raises(ValueError):
            TriageAgentConfig(llm_api_key="key", llm_provider="grok")  # type: ignore[arg-type]

    def test_llm_base_url_defaults_to_local_endpoint(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.llm_base_url == "http://localhost:8000"

    def test_llm_base_url_from_env(self) -> None:
        with patch.dict(
            os.environ, {**_CLEAN_ENV, "LLM_BASE_URL": "http://vllm:8080/v1"}, clear=True
        ):
            config = TriageAgentConfig()
        assert config.llm_base_url == "http://vllm:8080/v1"


class TestGetConfigSingleton:
    """get_config() returns singleton via lru_cache."""

    def test_returns_triage_agent_config(self) -> None:
        """get_config() should return a TriageAgentConfig instance."""
        get_config.cache_clear()

        with patch.dict(os.environ, {**_CLEAN_ENV, "LLM_API_KEY": "test-key"}, clear=True):
            config = get_config()

        assert isinstance(config, TriageAgentConfig)

    def test_same_instance_on_repeat_calls(self) -> None:
        """Repeated get_config() calls should return the same object (identity)."""
        get_config.cache_clear()

        with patch.dict(os.environ, {**_CLEAN_ENV, "LLM_API_KEY": "test-key"}, clear=True):
            config1 = get_config()
            config2 = get_config()

        assert config1 is config2

    def test_cache_clear_yields_new_instance(self) -> None:
        """After cache_clear(), get_config() should create a fresh instance."""
        get_config.cache_clear()

        with patch.dict(os.environ, {**_CLEAN_ENV, "LLM_API_KEY": "key-1"}, clear=True):
            first = get_config()

        get_config.cache_clear()

        with patch.dict(os.environ, {**_CLEAN_ENV, "LLM_API_KEY": "key-2"}, clear=True):
            second = get_config()

        assert first is not second
        assert first.llm_api_key == "key-1"
        assert second.llm_api_key == "key-2"


class TestNewModelConfigFields:
    """Tests for newly added model_config fields."""

    def test_llm_temperature_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.llm_temperature == 0.1

    def test_llm_temperature_from_env(self) -> None:
        with patch.dict(os.environ, {**_CLEAN_ENV, "LLM_TEMPERATURE": "0.3"}, clear=True):
            config = TriageAgentConfig()
        assert config.llm_temperature == 0.3


class TestNewAgentConfigFields:
    """Tests for newly added agent_config fields."""

    def test_max_attempts_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.max_attempts == 2

    def test_max_attempts_from_env(self) -> None:
        with patch.dict(os.environ, {**_CLEAN_ENV, "MAX_ATTEMPTS": "3"}, clear=True):
            config = TriageAgentConfig()
        assert config.max_attempts == 3

    def test_alert_window_defaults(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.alert_lookback_seconds == 300
        assert config.alert_lookahead_seconds == 60

    def test_imsi_windows_defaults(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.imsi_discovery_window_seconds == 30
        assert config.imsi_trace_lookback_seconds == 120
        assert config.imsi_digit_length == 15

    def test_confidence_gate_defaults(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.min_confidence_default == 0.70
        assert config.min_confidence_relaxed == 0.65
        assert config.high_evidence_threshold == 0.80


class TestNewScoringConfigFields:
    """Tests for newly added scoring_config fields."""

    def test_infra_weight_defaults_sum_to_one(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        total = (
            config.infra_weight_restarts
            + config.infra_weight_oom
            + config.infra_weight_pod_status
            + config.infra_weight_resources
        )
        assert abs(total - 1.0) < 1e-9

    def test_restart_thresholds_ordered(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.restart_threshold_high < config.restart_threshold_critical

    def test_evidence_quality_scores_defaults(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.eq_score_all_sources == 0.95
        assert config.eq_score_metrics_logs == 0.80
        assert config.eq_score_no_evidence == 0.10

    def test_rca_thresholds_defaults(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.infra_root_cause_threshold == 0.80
        assert config.infra_triggered_threshold == 0.60
        assert config.app_only_threshold == 0.30

    def test_scoring_fields_overridable_via_env(self) -> None:
        with patch.dict(
            os.environ,
            {**_CLEAN_ENV, "INFRA_WEIGHT_RESTARTS": "0.50", "INFRA_WEIGHT_OOM": "0.10",
             "INFRA_WEIGHT_POD_STATUS": "0.20", "INFRA_WEIGHT_RESOURCES": "0.20"},
            clear=True,
        ):
            config = TriageAgentConfig()
        assert config.infra_weight_restarts == 0.50
        assert config.infra_weight_oom == 0.10


class TestNewQueryConfigFields:
    """Tests for newly added query_config fields."""

    def test_promql_window_defaults(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.promql_restart_window == "1h"
        assert config.promql_oom_window == "5m"
        assert config.promql_cpu_rate_window_infra == "2m"
        assert config.promql_error_rate_window == "1m"
        assert config.promql_cpu_rate_window_nf == "5m"
        assert config.promql_range_step == "15s"
        assert config.loki_query_limit == 1000

    def test_loki_query_limit_from_env(self) -> None:
        with patch.dict(os.environ, {**_CLEAN_ENV, "LOKI_QUERY_LIMIT": "5000"}, clear=True):
            config = TriageAgentConfig()
        assert config.loki_query_limit == 5000


class TestNewInfraConfigFields:
    """Tests for newly added infra_config fields."""

    def test_memgraph_pool_size_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.memgraph_pool_size == 10

    def test_memgraph_max_retries_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.memgraph_max_retries == 3

    def test_known_nfs_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert "amf" in config.known_nfs
        assert "smf" in config.known_nfs
        assert len(config.known_nfs) == 9

    def test_known_nfs_from_env(self) -> None:
        with patch.dict(
            os.environ,
            {**_CLEAN_ENV, 'KNOWN_NFS': '["amf","smf","custom-nf"]'},
            clear=True,
        ):
            config = TriageAgentConfig()
        assert config.known_nfs == ["amf", "smf", "custom-nf"]


class TestNewApiConfigFields:
    """Tests for newly added api_config fields."""

    def test_prometheus_max_retries_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.prometheus_max_retries == 3

    def test_cors_allow_origins_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.cors_allow_origins == ["*"]

    def test_cors_allow_origins_from_env(self) -> None:
        with patch.dict(
            os.environ,
            {**_CLEAN_ENV, 'CORS_ALLOW_ORIGINS': '["http://alertmanager:9093"]'},
            clear=True,
        ):
            config = TriageAgentConfig()
        assert config.cors_allow_origins == ["http://alertmanager:9093"]

    def test_server_host_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.server_host == "0.0.0.0"

    def test_server_port_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.server_port == 8000


class TestObservabilityConfig:
    """Tests for observability_config fields."""

    def test_app_version_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.app_version == "3.2.0"

    def test_app_version_from_env(self) -> None:
        with patch.dict(os.environ, {**_CLEAN_ENV, "APP_VERSION": "4.0.0"}, clear=True):
            config = TriageAgentConfig()
        assert config.app_version == "4.0.0"


class TestLangSmithConfig:
    """LangSmith tracing env var wiring via model_validator."""

    def test_env_vars_set_when_tracing_enabled(self) -> None:
        """LANGCHAIN_TRACING_V2=true wires env vars into os.environ."""
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            TriageAgentConfig(
                langchain_tracing_v2="true",
                langsmith_api_key="sk-test",
                langchain_project="my-project",
                langchain_endpoint="https://api.smith.langchain.com",
            )
            assert os.environ.get("LANGCHAIN_TRACING_V2") == "true"
            assert os.environ.get("LANGCHAIN_PROJECT") == "my-project"
            assert os.environ.get("LANGSMITH_API_KEY") == "sk-test"

    def test_env_vars_not_set_when_tracing_disabled(self) -> None:
        """LANGCHAIN_TRACING_V2=false must not force env vars to 'true'."""
        env = {**_CLEAN_ENV}
        env.pop("LANGCHAIN_TRACING_V2", None)
        with patch.dict(os.environ, env, clear=True):
            TriageAgentConfig(langchain_tracing_v2="false")
            assert os.environ.get("LANGCHAIN_TRACING_V2") != "true"

    def test_langchain_project_defaults_to_5g_triage_agent(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.langchain_project == "5g-triage-agent"

    def test_existing_env_var_not_overwritten(self) -> None:
        """setdefault semantics: pre-existing env var wins over config value."""
        env = {**_CLEAN_ENV, "LANGCHAIN_PROJECT": "pre-existing"}
        with patch.dict(os.environ, env, clear=True):
            TriageAgentConfig(
                langchain_tracing_v2="true",
                langchain_project="config-value",
            )
            assert os.environ.get("LANGCHAIN_PROJECT") == "pre-existing"


class TestArtifactsConfig:
    """Tests for artifacts_dir and nf_latency_threshold_seconds fields."""

    def test_artifacts_dir_default(self) -> None:
        from pathlib import Path
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert Path(config.artifacts_dir).is_absolute()
        assert config.artifacts_dir.endswith("artifacts")

    def test_artifacts_dir_from_env(self) -> None:
        with patch.dict(os.environ, {**_CLEAN_ENV, "ARTIFACTS_DIR": "/tmp/data"}, clear=True):
            config = TriageAgentConfig()
        assert config.artifacts_dir == "/tmp/data"

    def test_nf_latency_threshold_default(self) -> None:
        with patch.dict(os.environ, _CLEAN_ENV, clear=True):
            config = TriageAgentConfig(llm_api_key="test-key")
        assert config.nf_latency_threshold_seconds == 1.0

    def test_nf_latency_threshold_from_env(self) -> None:
        with patch.dict(
            os.environ, {**_CLEAN_ENV, "NF_LATENCY_THRESHOLD_SECONDS": "2.5"}, clear=True
        ):
            config = TriageAgentConfig()
        assert config.nf_latency_threshold_seconds == 2.5


class TestLogNoisePatterns:
    """Tests for log_noise_patterns configuration field."""

    def test_config_has_log_noise_patterns(self) -> None:
        """log_noise_patterns field must exist and be a list."""
        from triage_agent.config import get_config

        cfg = get_config()
        assert hasattr(cfg, "log_noise_patterns")
        assert isinstance(cfg.log_noise_patterns, list)
        assert len(cfg.log_noise_patterns) >= 1

    def test_config_log_noise_patterns_cover_bsf(self) -> None:
        """At least one pattern must reference BSF noise."""
        from triage_agent.config import get_config

        cfg = get_config()
        bsf_patterns = [
            p for p in cfg.log_noise_patterns if "BSF" in p or "bsf" in p.lower()
        ]
        assert bsf_patterns, "Expected at least one BSF noise pattern in config"

    def test_log_noise_patterns_override_via_constructor(self) -> None:
        """log_noise_patterns can be overridden via constructor."""
        from triage_agent.config import TriageAgentConfig

        patterns = ["*custom noise*", "*test pattern*"]
        cfg = TriageAgentConfig(log_noise_patterns=patterns)
        assert cfg.log_noise_patterns == patterns


class TestArtifactsDirResolution:
    def test_relative_artifacts_dir_is_resolved_to_absolute(self) -> None:
        """A relative artifacts_dir must be converted to absolute at config load."""
        from triage_agent.config import TriageAgentConfig
        from pathlib import Path
        cfg = TriageAgentConfig(artifacts_dir="artifacts")
        assert Path(cfg.artifacts_dir).is_absolute()

    def test_absolute_artifacts_dir_unchanged(self) -> None:
        from triage_agent.config import TriageAgentConfig
        cfg = TriageAgentConfig(artifacts_dir="/tmp/my_artifacts")
        assert cfg.artifacts_dir == "/tmp/my_artifacts"
