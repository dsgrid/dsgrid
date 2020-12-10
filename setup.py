"""
setup.py
"""
from codecs import open
from setuptools import setup, find_packages
import os
import logging
import shlex

try:
    from pypandoc import convert_text
except ImportError:
    convert_text = lambda string, *args, **kwargs: string


logger = logging.getLogger(__name__)


def read_lines(filename):
    with open(filename) as f_in:
        return f_in.readlines()


here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", encoding="utf-8") as readme_file:
    readme = convert_text(readme_file.read(), "rst", format="md")

with open(os.path.join(here, "dsgrid", "version.py"), encoding="utf-8") as f:
    version = f.read()

version = version.split()[2].strip('"').strip("'")

test_requires = ["pytest"]

setup(
    name="dsgrid",
    version=version,
    description="dsgrid",
    long_description=readme,
    author="NREL",
    maintainer_email="elaine.hale@nrel.gov",
    url="https://github.com/dsgrid/dsgrid.git",
    packages=find_packages(),
    package_dir={"dsgrid": "dsgrid"},
    entry_points={
        "console_scripts": [
            "dsgrid=dsgrid.cli.dsgrid:cli",
        ],
    },
    include_package_data=True,
    license="BSD license",
    zip_safe=False,
    keywords="dsgrid",
    classifiers=[
        "Development Status :: Alpha",
        "Intended Audience :: Modelers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.8",
    ],
    test_suite="tests",
    install_requires=read_lines("requirements.txt"),
)
