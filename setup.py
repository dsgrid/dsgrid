"""
setup.py
"""
import logging
from pathlib import Path
from setuptools import setup, find_packages

logger = logging.getLogger(__name__)

here = Path(__file__).parent.resolve()
metadata = {}

with open(here / "dsgrid" / "_version.py", encoding="utf-8") as f:
    exec(f.read(), metadata)

with open(here / "README.md", encoding="utf-8") as f:
    readme = f.read()

dev_requires = ["black", "pre-commit", "devtools", "jupyter", "flake8"]

test_requires = ["pytest", "pytest-cov"]

doc_requires = [
    "ghp-import",
    "numpydoc",
    "pandoc",
    "sphinx",
    "sphinx_rtd_theme",
    "sphinx_argparse",
    "sphinxcontrib.programoutput",
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
        "awscli",
        "boto3",
        "click>=8",
        "fastapi",
        "numpy",
        "pandas",
        "prettytable",
        "pydantic",
        "pyspark==3.3",  # Keep this synced with the spark version in Dockerfile.
        "requests",
        "s3path",
        "semver",
        "sqlalchemy",
        "toml",
        "uvicorn",
        "tzdata", # time zone stuff
        "pyarrow",
    ],
    extras_require={
        "test": test_requires,
        "dev": test_requires + dev_requires,
        "admin": test_requires + dev_requires + doc_requires + release_requires,
    },
)
