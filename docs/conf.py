"""Sphinx configuration for 5G TriageAgent documentation."""

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

# ---------------------------------------------------------------------------
# Project metadata
# ---------------------------------------------------------------------------

project = "5G TriageAgent"
copyright = "2024, TriageAgent Contributors"
author = "TriageAgent Contributors"
release = "3.2.0"

# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
    "autoapi.extension",
    "myst_parser",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]

# ---------------------------------------------------------------------------
# Source files
# ---------------------------------------------------------------------------

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "plans",
    "superpowers",
    "README.md",
    "triageagent_architecture_design2.archive.md",
    "claude-code-cli-workflow.md",
]

# ---------------------------------------------------------------------------
# MyST (Markdown) settings
# ---------------------------------------------------------------------------

myst_enable_extensions = ["colon_fence", "deflist", "attrs_inline"]
myst_heading_anchors = 3

# ---------------------------------------------------------------------------
# AutoAPI — discovers src/triage_agent automatically
# ---------------------------------------------------------------------------

autoapi_dirs = ["../src"]
autoapi_type = "python"
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
]
autoapi_ignore = ["*/migrations/*", "*/tests/*"]

# ---------------------------------------------------------------------------
# autodoc / typehints
# ---------------------------------------------------------------------------

autodoc_typehints = "description"
autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# ---------------------------------------------------------------------------
# Intersphinx — link to Python stdlib docs
# ---------------------------------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# ---------------------------------------------------------------------------
# HTML output
# ---------------------------------------------------------------------------

html_theme = "furo"
html_static_path = []
html_title = "5G TriageAgent"
