# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import sys
from pathlib import Path

# Add parent directory to path for imports
docs_dir = Path(__file__).parent.parent
sys.path.insert(0, str(docs_dir))

# -- Project information -----------------------------------------------------
import dsgrid

project = dsgrid.__name__
copyright = dsgrid.__copyright__
author = dsgrid.__author__

# The short X.Y version
version = dsgrid.__version__
# The full version, including alpha/beta/rc tags
release = dsgrid.__version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.coverage",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "sphinx.ext.napoleon",
    # "sphinx.ext.autosectionlabel",  # Temporarily disabled due to NoneType parent error
    "sphinxarg.ext",
    "sphinx.ext.todo",
    "sphinxcontrib.programoutput",
    "sphinx_copybutton",
    "sphinx_click",
    "sphinx_design",
    "sphinxcontrib.autodoc_pydantic",
    "sphinx_tabs.tabs",
]

# MyST parser configuration
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
    "html_admonition",
    "html_image",
    "linkify",
    "replacements",
    "smartquotes",
    "substitution",
    "tasklist",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

# The name of the Pygments (syntax highlighting) style to use.
# pygments_style = "sphinx"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
html_theme_options = {
    "navigation_with_keys": True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]


# -- Extension configuration -------------------------------------------------

autoclass_content = "both"
autodoc_member_order = "bysource"
# autodoc_special_members = ['__getitem__', '__setitem__','__iter__']
numpy_show_class_member = True
todo_include_todos = True


# CSS styling
rst_prolog = """
.. include:: /special.rst

"""

html_css_files = ["style.css"]

autosectionlabel_prefix_document = True
autosectionlabel_maxdepth = 100
copybutton_only_copy_prompt_lines = True
copybutton_exclude = ".linenos, .gp, .go"
copybutton_line_continuation_character = "\\"
copybutton_here_doc_delimiter = "EOT"
copybutton_prompt_text = "$"
copybutton_copy_empty_lines = False
autodoc_pydantic_model_show_json = False
autodoc_pydantic_model_show_config_member = False
autodoc_pydantic_model_show_config_summary = False
autodoc_pydantic_model_erdantic_figure = False
autodoc_pydantic_model_erdantic_figure_collapsed = False

# Display Pydantic model fields in compact table format
autodoc_pydantic_model_show_field_summary = True
autodoc_pydantic_model_summary_list_order = "bysource"
autodoc_pydantic_field_list_validators = True
autodoc_pydantic_field_doc_policy = "docstring"
autodoc_pydantic_field_show_constraints = True
autodoc_pydantic_field_show_alias = False
autodoc_pydantic_field_show_default = True
autodoc_pydantic_model_hide_paramlist = True
autodoc_pydantic_model_members = True
autodoc_pydantic_model_undoc_members = False


# -- Custom setup for auto-generating model documentation -------------------


def setup(app):
    """Sphinx setup hook to auto-generate data model documentation."""
    # Generate enum documentation first
    try:
        from doc_generators.generate_enums import main as generate_enums

        print("Generating enum documentation...")
        result = generate_enums()
        if result != 0:
            print("Warning: Failed to generate some enum documentation")
    except Exception as e:
        print(f"Warning: Could not generate enum documentation: {e}")
        # Don't fail the build if generation fails

    # Generate data model documentation
    try:
        from doc_generators.generate_all_models import main as generate_models

        print("Generating data model documentation...")
        result = generate_models()
        if result != 0:
            print("Warning: Failed to generate some model documentation")
    except Exception as e:
        print(f"Warning: Could not generate model documentation: {e}")
        # Don't fail the build if generation fails
