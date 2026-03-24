"""Tests for multi-LLM provider factory and CLI entry point.

Covers:
- create_llm() factory: all three providers, edge cases, error paths
- llm_analyze_evidence(): verify it delegates to create_llm() with correct args
- __main__.py CLI: --llm-provider arg sets LLM_PROVIDER env var before uvicorn
"""

import importlib
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from langchain_openai import ChatOpenAI

from triage_agent.agents.rca_agent import create_llm


class TestCreateLLMFactory:
    """Tests for the create_llm() provider factory."""

    def test_openai_returns_chat_openai(self) -> None:
        llm = create_llm("openai", "gpt-4o-mini", "sk-test", 30)
        assert isinstance(llm, ChatOpenAI)

    def test_openai_with_api_key(self) -> None:
        llm = create_llm("openai", "gpt-4o-mini", "sk-test-key", 30)
        assert isinstance(llm, ChatOpenAI)

    def test_openai_with_empty_api_key(self) -> None:
        """Empty api_key lets ChatOpenAI fall back to OPENAI_API_KEY env var."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-env-based"}):
            llm = create_llm("openai", "gpt-4o-mini", "", 30)
        assert isinstance(llm, ChatOpenAI)

    def test_local_returns_chat_openai(self) -> None:
        llm = create_llm("local", "mistral-7b", "", 60, base_url="http://vllm:8080/v1")
        assert isinstance(llm, ChatOpenAI)

    def test_local_raises_without_base_url(self) -> None:
        with pytest.raises(ValueError, match="llm_base_url must be set"):
            create_llm("local", "mistral-7b", "", 60, base_url="")

    def test_local_raises_with_empty_string_base_url(self) -> None:
        with pytest.raises(ValueError, match="llm_base_url must be set"):
            create_llm("local", "llama3", "ignored", 30)

    def test_anthropic_raises_import_error_without_package(self) -> None:
        """If langchain-anthropic not installed, raise ImportError with install hint."""
        with patch.dict("sys.modules", {"langchain_anthropic": None}):  # type: ignore[dict-item]
            with pytest.raises(ImportError, match="pip install triage-agent"):
                create_llm("anthropic", "claude-opus-4-6", "sk-ant-test", 30)

    @pytest.mark.skipif(
        importlib.util.find_spec("langchain_anthropic") is None,
        reason="langchain-anthropic not installed",
    )
    def test_anthropic_returns_chat_anthropic(self) -> None:
        from langchain_anthropic import ChatAnthropic

        llm = create_llm("anthropic", "claude-opus-4-6", "sk-ant-test", 30)
        assert isinstance(llm, ChatAnthropic)

    def test_groq_returns_chat_openai(self) -> None:
        llm = create_llm("groq", "openai/gpt-oss-20b", "", 30, groq_api_key="gsk_test")
        assert isinstance(llm, ChatOpenAI)

    def test_groq_raises_without_api_key(self) -> None:
        with pytest.raises(ValueError, match="groq_api_key must be set"):
            create_llm("groq", "openai/gpt-oss-20b", "", 30, groq_api_key="")

    def test_groq_reasoning_effort_included_in_model_kwargs(self) -> None:
        llm = create_llm("groq", "openai/gpt-oss-20b", "", 30, groq_api_key="gsk_test", reasoning_effort="medium")
        # Newer langchain-openai promotes known params out of model_kwargs into top-level attributes.
        has_effort = (
            llm.model_kwargs.get("reasoning_effort") == "medium"
            or getattr(llm, "reasoning_effort", None) == "medium"
        )
        assert has_effort, f"reasoning_effort not found on llm; model_kwargs={llm.model_kwargs}"

    def test_groq_empty_reasoning_effort_omitted_from_model_kwargs(self) -> None:
        llm = create_llm("groq", "openai/gpt-oss-20b", "", 30, groq_api_key="gsk_test", reasoning_effort="")
        assert "reasoning_effort" not in llm.model_kwargs

    def test_groq_uses_groq_base_url(self) -> None:
        llm = create_llm("groq", "openai/gpt-oss-20b", "", 30, groq_api_key="gsk_test")
        assert llm.openai_api_base == "https://api.groq.com/openai/v1"

    def test_unknown_provider_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unsupported llm_provider"):
            create_llm("grok", "some-model", "key", 30)

    def test_unknown_provider_message_includes_name(self) -> None:
        with pytest.raises(ValueError, match="grok"):
            create_llm("grok", "some-model", "key", 30)


class TestLLMAnalyzeEvidenceUsesProvider:
    """Tests that llm_analyze_evidence() delegates to create_llm() correctly."""

    def _make_mock_config(
        self,
        provider: str = "openai",
        model: str = "gpt-4o-mini",
        api_key: str = "sk-test",
        timeout: int = 30,
        base_url: str = "",
        groq_api_key: str = "",
        groq_reasoning_effort: str = "",
    ) -> MagicMock:
        cfg = MagicMock()
        cfg.llm_provider = provider
        cfg.llm_model = model
        cfg.llm_api_key = api_key
        cfg.llm_timeout = timeout
        cfg.llm_base_url = base_url
        cfg.groq_api_key = groq_api_key
        cfg.groq_reasoning_effort = groq_reasoning_effort
        return cfg

    def _make_mock_response(self, content: str = '{"root_nf": "AMF", "failure_mode": "timeout", "confidence": 0.8}') -> MagicMock:
        r = MagicMock()
        r.content = content
        return r

    def test_openai_provider_calls_create_llm_with_correct_args(self) -> None:
        from triage_agent.agents.rca_agent import llm_analyze_evidence

        mock_llm = MagicMock()
        mock_llm.invoke.return_value = self._make_mock_response()

        with (
            patch("triage_agent.agents.rca_agent.create_llm", return_value=mock_llm) as mock_factory,
            patch("triage_agent.agents.rca_agent.get_config", return_value=self._make_mock_config()),
        ):
            llm_analyze_evidence("test prompt")

        mock_factory.assert_called_once_with(
            provider="openai",
            model="gpt-4o-mini",
            api_key="sk-test",
            timeout=30,
            base_url="",
            groq_api_key="",
            reasoning_effort="",
        )

    def test_anthropic_provider_calls_create_llm_with_correct_args(self) -> None:
        from triage_agent.agents.rca_agent import llm_analyze_evidence

        mock_llm = MagicMock()
        mock_llm.invoke.return_value = self._make_mock_response()

        with (
            patch("triage_agent.agents.rca_agent.create_llm", return_value=mock_llm) as mock_factory,
            patch(
                "triage_agent.agents.rca_agent.get_config",
                return_value=self._make_mock_config(provider="anthropic", model="claude-opus-4-6", api_key="sk-ant-key"),
            ),
        ):
            llm_analyze_evidence("test prompt")

        mock_factory.assert_called_once_with(
            provider="anthropic",
            model="claude-opus-4-6",
            api_key="sk-ant-key",
            timeout=30,
            base_url="",
            groq_api_key="",
            reasoning_effort="",
        )

    def test_groq_provider_calls_create_llm_with_correct_args(self) -> None:
        from triage_agent.agents.rca_agent import llm_analyze_evidence

        mock_llm = MagicMock()
        mock_llm.invoke.return_value = self._make_mock_response()

        cfg = self._make_mock_config(provider="groq", model="openai/gpt-oss-20b", api_key="")
        cfg.groq_api_key = "gsk_test"
        cfg.groq_reasoning_effort = "medium"

        with (
            patch("triage_agent.agents.rca_agent.create_llm", return_value=mock_llm) as mock_factory,
            patch("triage_agent.agents.rca_agent.get_config", return_value=cfg),
        ):
            llm_analyze_evidence("test prompt")

        mock_factory.assert_called_once_with(
            provider="groq",
            model="openai/gpt-oss-20b",
            api_key="",
            timeout=30,
            base_url="",
            groq_api_key="gsk_test",
            reasoning_effort="medium",
        )

    def test_local_provider_passes_base_url(self) -> None:
        from triage_agent.agents.rca_agent import llm_analyze_evidence

        mock_llm = MagicMock()
        mock_llm.invoke.return_value = self._make_mock_response()

        with (
            patch("triage_agent.agents.rca_agent.create_llm", return_value=mock_llm) as mock_factory,
            patch(
                "triage_agent.agents.rca_agent.get_config",
                return_value=self._make_mock_config(
                    provider="local",
                    model="mistral-7b",
                    api_key="",
                    base_url="http://vllm:8080/v1",
                ),
            ),
        ):
            llm_analyze_evidence("test prompt")

        mock_factory.assert_called_once_with(
            provider="local",
            model="mistral-7b",
            api_key="",
            timeout=30,
            base_url="http://vllm:8080/v1",
            groq_api_key="",
            reasoning_effort="",
        )

    def test_explicit_timeout_overrides_config_timeout(self) -> None:
        from triage_agent.agents.rca_agent import llm_analyze_evidence

        mock_llm = MagicMock()
        mock_llm.invoke.return_value = self._make_mock_response()

        with (
            patch("triage_agent.agents.rca_agent.create_llm", return_value=mock_llm) as mock_factory,
            patch("triage_agent.agents.rca_agent.get_config", return_value=self._make_mock_config(timeout=30)),
        ):
            llm_analyze_evidence("test prompt", timeout=60)

        # Explicit timeout=60 overrides config.llm_timeout=30
        call_kwargs = mock_factory.call_args.kwargs
        assert call_kwargs["timeout"] == 60


class TestMainCLI:
    """Tests for __main__.py CLI argument handling."""

    def _reload_main(self) -> object:
        """Reload __main__ to avoid import caching issues."""
        if "triage_agent.__main__" in sys.modules:
            del sys.modules["triage_agent.__main__"]
        import triage_agent.__main__ as m
        importlib.reload(m)
        return m

    def test_llm_provider_arg_sets_env_var(self) -> None:
        """--llm-provider anthropic should set LLM_PROVIDER=anthropic env var."""
        m = self._reload_main()
        with (
            patch("sys.argv", ["triage_agent", "--llm-provider", "anthropic"]),
            patch("uvicorn.run"),
            patch.dict(os.environ, {}, clear=False),
        ):
            m.main()  # type: ignore[attr-defined]
            # Check INSIDE the context before patch.dict restores the original env
            assert os.environ.get("LLM_PROVIDER") == "anthropic"

    def test_llm_provider_groq_sets_env_var(self) -> None:
        m = self._reload_main()
        with (
            patch("sys.argv", ["triage_agent", "--llm-provider", "groq"]),
            patch("uvicorn.run"),
            patch.dict(os.environ, {}, clear=False),
        ):
            m.main()  # type: ignore[attr-defined]
            assert os.environ.get("LLM_PROVIDER") == "groq"

    def test_llm_provider_local_sets_env_var(self) -> None:
        m = self._reload_main()
        with (
            patch("sys.argv", ["triage_agent", "--llm-provider", "local"]),
            patch("uvicorn.run"),
            patch.dict(os.environ, {}, clear=False),
        ):
            m.main()  # type: ignore[attr-defined]
            assert os.environ.get("LLM_PROVIDER") == "local"

    def test_no_llm_provider_arg_does_not_override_env(self) -> None:
        """Without --llm-provider, LLM_PROVIDER env var should not be forced."""
        m = self._reload_main()
        with (
            patch("sys.argv", ["triage_agent"]),
            patch("uvicorn.run"),
            patch.dict(os.environ, {"LLM_PROVIDER": "anthropic"}, clear=False),
        ):
            m.main()  # type: ignore[attr-defined]
            # Should stay as "anthropic" — not overridden by absent --llm-provider
            assert os.environ.get("LLM_PROVIDER") == "anthropic"

    def test_invalid_provider_arg_exits(self) -> None:
        """argparse should exit with error for invalid --llm-provider value."""
        m = self._reload_main()
        with (
            patch("sys.argv", ["triage_agent", "--llm-provider", "grok"]),
            pytest.raises(SystemExit),
        ):
            m.main()  # type: ignore[attr-defined]

    def test_uvicorn_run_called_with_correct_defaults(self) -> None:
        """uvicorn.run should be called with default host and port."""
        m = self._reload_main()
        with (
            patch("sys.argv", ["triage_agent"]),
            patch("uvicorn.run") as mock_run,
        ):
            m.main()  # type: ignore[attr-defined]
        mock_run.assert_called_once_with(
            "triage_agent.api.webhook:app",
            host="0.0.0.0",
            port=8000,
            reload=False,
        )

    def test_custom_host_and_port_passed_to_uvicorn(self) -> None:
        m = self._reload_main()
        with (
            patch("sys.argv", ["triage_agent", "--host", "127.0.0.1", "--port", "9000"]),
            patch("uvicorn.run") as mock_run,
        ):
            m.main()  # type: ignore[attr-defined]
        mock_run.assert_called_once_with(
            "triage_agent.api.webhook:app",
            host="127.0.0.1",
            port=9000,
            reload=False,
        )
