"""
setup.py
"""
import logging
from pathlib import Path
from setuptools import setup, find_packages

from pygments.lexers.shell import BashSessionLexer, PowerShellSessionLexer

logger = logging.getLogger(__name__)

here = Path(__file__).parent.resolve()
metadata = {}

with open(here / "dsgrid" / "_version.py", encoding="utf-8") as f:
    exec(f.read(), metadata)

with open(here / "README.md", encoding="utf-8") as f:
    readme = f.read()

dev_requires = ["black>=22.3.0", "pre-commit", "devtools", "flake8", "pyarrow"]

test_requires = [
    "httpx",  # starlette, used by fastapi, requires this as an optional dependency for testing.
    "pytest",
    "pytest-cov",
]

doc_requires = [
    "furo",
    "ghp-import",
    "numpydoc",
    "sphinx~=7.2",
    "sphinx-click~=5.0",
    "sphinx-copybutton~=0.5.2",
    "sphinx-tabs~=3.4",
    "sphinx_argparse~=0.4.0",
    "sphinxcontrib.programoutput",
    "autodoc_pydantic[erdantic]~=1.9",
]

release_requires = ["twine", "setuptools", "wheel"]

setup(
    name=metadata["__title__"],
    version=metadata["__version__"],
    description=metadata["__description__"],
    long_description=readme,
    long_description_content_type="text/markdown",
    author=metadata["__author__"],
    maintainer_email=metadata["__maintainer_email__"],
    url=metadata["__url__"],
    packages=find_packages(),
    package_dir={"dsgrid": "dsgrid"},
    package_data={
        "dsgrid": [
            "notebooks/*.ipynb",
        ]
    },
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "dsgrid=dsgrid.cli.dsgrid:cli",
            # This exists because spark-submit does not recognize the above 'dsgrid' as a Python
            # application.
            "dsgrid-cli.py=dsgrid.cli.dsgrid:cli",
            "dsgrid-admin=dsgrid.cli.dsgrid_admin:cli",
        ],
    },
    include_package_data=True,
    license=metadata["__license__"],
    zip_safe=False,
    keywords="dsgrid",
    classifiers=[
        "Development Status :: Alpha",
        "Intended Audience :: Modelers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.10",
    ],
    test_suite="tests",
    install_requires=[
        "awscliv2",
        "boto3",
        "click>=8",
        "dash",
        "dash_bootstrap_components",
        "fastapi",
        "json5",
        "pandas",
        "prettytable",
        "pydantic~=1.10.11",
        "pyspark==3.4.1",  # Keep this synced with the spark version in Dockerfile.
        "python-arango",
        "requests",
        "s3path",
        "semver",
        "sqlalchemy",
        "uvicorn",
        "tzdata",  # time zone stuff
    ],
    extras_require={
        "test": test_requires,
        "dev": test_requires + dev_requires,
        "admin": test_requires + dev_requires + doc_requires + release_requires,
    },
)
