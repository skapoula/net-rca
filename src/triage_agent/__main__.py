"""CLI entry point for 5G TriageAgent.

Allows running via:
    python -m triage_agent [--llm-provider PROVIDER] [--host HOST] [--port PORT]
    triage-agent [--llm-provider PROVIDER]            # after pip install

The --llm-provider argument sets LLM_PROVIDER in os.environ before uvicorn
starts, so TriageAgentConfig (a BaseSettings lru_cache singleton) picks it up
at first call. The existing uvicorn invocation continues to work unchanged.

Examples:
    python -m triage_agent --llm-provider anthropic
    python -m triage_agent --llm-provider local
    python -m triage_agent --llm-provider openai --port 9000
    LLM_PROVIDER=local LLM_BASE_URL=http://vllm:8080/v1 python -m triage_agent
"""

import argparse
import os

# Import lazily inside main() to ensure LLM_PROVIDER env var is set first.
# get_config() is an lru_cache singleton; importing config here is safe because
# we read .server_host / .server_port before the cache is populated.


def main() -> None:
    """Parse CLI args and start the uvicorn webhook server."""
    parser = argparse.ArgumentParser(
        description="5G TriageAgent webhook service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--llm-provider",
        choices=["openai", "anthropic", "local", "groq"],
        default=None,
        help=(
            "LLM provider to use. Overrides LLM_PROVIDER env var. "
            "groq=Groq inference API (default), openai=ChatGPT, "
            "anthropic=Claude, local=vLLM/Ollama in-cluster pod."
        ),
    )
    from triage_agent.config import get_config as _get_config  # noqa: PLC0415
    _defaults = _get_config()
    parser.add_argument(
        "--host",
        default=_defaults.server_host,
        help="Host to bind the webhook server to",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=_defaults.server_port,
        help="Port to bind the webhook server to",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        default=False,
        help="Enable auto-reload for development",
    )

    args = parser.parse_args()

    # Set LLM_PROVIDER env var BEFORE uvicorn imports the app.
    # TriageAgentConfig is an lru_cache singleton that reads env vars at first
    # call (not at import time), so this ordering guarantee holds.
    if args.llm_provider is not None:
        os.environ["LLM_PROVIDER"] = args.llm_provider

    import uvicorn

    uvicorn.run(
        "triage_agent.api.webhook:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
