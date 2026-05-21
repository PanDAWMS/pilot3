# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

"""Sphinx configuration for PanDA Pilot 3 documentation."""

import os
import sys

# Make the pilot package importable without installing it.
sys.path.insert(0, os.path.abspath('..'))

# -- Project information -----------------------------------------------------
project = 'PanDA Pilot 3'
copyright = '2024, ATLAS PanDA Collaboration'
author = 'Paul Nilsson'

# Read the version from the canonical PILOTVERSION file so it never drifts.
_version_file = os.path.join(os.path.dirname(__file__), '..', 'PILOTVERSION')
try:
    with open(_version_file) as _f:
        _full_version = _f.read().strip()
except FileNotFoundError:
    _full_version = 'unknown'

version = '.'.join(_full_version.split('.')[:2])   # e.g. "3.12"
release = _full_version                             # e.g. "3.12.5.5"

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',       # pull docstrings from source
    'sphinx.ext.napoleon',      # parse Google-style docstrings
    'sphinx.ext.viewcode',      # add [source] links to API pages
    'sphinx.ext.intersphinx',   # cross-link to Python stdlib docs
    'sphinx.ext.autosummary',   # generate summary tables automatically
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Napoleon (Google-style docstring) settings ------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_attr_annotations = True

# -- Autodoc settings --------------------------------------------------------
autodoc_default_options = {
    'members': True,
    'undoc-members': False,
    'show-inheritance': True,
    'member-order': 'bysource',
}
autodoc_typehints = 'description'  # put type hints in the description, not the signature
autodoc_typehints_format = 'short'
add_module_names = False           # omit the full package path from class/function names

# Mock optional runtime dependencies that are not available in the doc-build environment.
# This allows autodoc to import and document modules that have bare third-party imports
# (e.g. ROOT, which has no pip package) without failing.
autodoc_mock_imports = ['ROOT', 'psutil']

# -- Autosummary settings ----------------------------------------------------
autosummary_generate = True

# -- Intersphinx mapping -----------------------------------------------------
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
}

# -- HTML output (furo theme) ------------------------------------------------
html_theme = 'furo'
html_title = f'PanDA Pilot 3 <span class="version">{release}</span>'
html_static_path = ['_static']

html_theme_options = {
    'sidebar_hide_name': False,
    'navigation_with_keys': True,
    'source_repository': 'https://github.com/PanDAWMS/pilot3/',
    'source_branch': 'master',
    'source_directory': 'doc/',
    'footer_icons': [
        {
            'name': 'GitHub',
            'url': 'https://github.com/PanDAWMS/pilot3',
            'html': (
                '<svg stroke="currentColor" fill="currentColor" stroke-width="0" '
                'viewBox="0 0 16 16"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 '
                '2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49'
                '-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.'
                '82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 '
                '0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-'
                '.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12'
                '.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-'
                '.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"'
                '></path></svg>'
            ),
            'class': '',
        },
    ],
}
