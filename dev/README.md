# dsgrid Developer Readme

## Developer Dependencies

**pip extras**

```
pip install -e .[tests]

# or

pip install -e .[dev] # includes what is needed for tests and code development

# or

pip install -e .[admin] # dev plus what is needed for creating documentation and releasing packages
```

**Setting up pre-commit hooks**

```
pre-commit install
```

**Additional software required for publishing documentation:**

- [Pandoc](https://pandoc.org/installing.html)

## Run Tests

In addition to grabbing the branch of dsgrid you want to test and making sure
you've activated an environment with dsgrid installed per the above, you'll
need to set up dsgrid for testing by selecting a dataset and setting
environment variables.

There are two datasets available:

1. https://github.com/dsgrid/dsgrid-test-data (Recommended for unit tests.)
   This is a stripped-down version of the EFS project and datasets. It is
   optimized for testing the dsgrid code, not for queries.

2. https://github.com/dsgrid/dsgrid-project-EFS (Recommended for queries.)
   This is the full version of the EFS project. The datasets must be
   downloaded from AWS.
   You can run most unit tests against this data. However, it is slow because
   the data is large.
   Some unit tests require specific files from the dsgrid-test-data and will
   be skipped if you run them on this data.

```
# Point to your checkout of a project repository (adjusting the path as needed).
export TEST_PROJECT_REPO=$HOME/dsgrid-test-data/test_efs
# or
export TEST_PROJECT_REPO=$HOME/dsgrid-project-EFS

# Point to your local dsgrid registry path.
# Feel free to use a different path for storing your test registry--this is just
# an example.
export DSGRID_REGISTRY_PATH=$HOME/.dsgrid-test-registry

# Point to your local directory of datasets. This data will be registered in the
# registry.
# Minimal test data
export DSGRID_LOCAL_DATA_DIRECTORY=$HOME/dsgrid-test-data/datasets
# Pre-downloaded EFS data 
export DSGRID_LOCAL_DATA_DIRECTORY=$HOME/.dsgrid-data

# This is what that directory should contain:
tree $DSGRID_LOCAL_DATA_DIRECTORY
.dsgrid-data
└── efs_comstock
    ├── convert_dsg.log
    ├── dimensions.json
    ├── enduse.csv
    ├── geography.csv
    ├── load_data.parquet
    │   ├── _SUCCESS
    │   ├── part-00000-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00001-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00002-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00003-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00004-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00005-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00006-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00007-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00008-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   └── part-00009-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    ├── load_data_lookup.parquet
    │   ├── _SUCCESS
    │   └── part-00000-69955796-0a87-4d5f-ba55-b8cbd4b1372d-c000.snappy.parquet
    ├── scale_factors.json
    ├── sector.csv
    ├── test.parquet
    └── time.csv
```

and then running:
```
python tests/make_us_data_registry.py $DSGRID_REGISTRY_PATH
```

After that you can run the tests:

```
cd dsgrid
pytest tests
```

pytest options that may be helpful:

option flag           | effect
--------------------- | ------
--log-cli-level=DEBUG | emits log messages to the console. level can be set to DEBUG, INFO, WARN, ERROR

## Interactive Exploration

In addition to the CLI tools you can use `scripts/registry.py` to explore a registry interactively.

Be sure to use the `debug` function from the `devtools` package when exploring Pydantic models.
```
ipython -i scripts/registry.py -- --path=$DSGRID_REGISTRY_PATH --offline
In [1]: manager.show()

In [2]: dataset = dataset_manager.get_by_id("efs_comstock")

In [3]: debug(dataset.model)
```

## Publish Documentation

The documentation is built with [Sphinx](http://sphinx-doc.org/index.html). There are several steps to creating and publishing the documentation:

1. Convert .md input files to .rst
2. Refresh API documentation
3. Build the HTML docs
4. Push to GitHub

### Markdown to reStructuredText

Markdown files are registered in `doc/md_files.txt`. Paths in that file should be relative to the docs folder and should exclude the file extension. For every file listed there, the `dev/md_to_rst.py` utility will expect to find a markdown (`.md`) file, and will look for an optional `.postfix` file, which is expected to contain `.rst` code to be appended to the `.rst` file created by converting the input `.md` file. Thus, running `dev/md_to_rst.py` on the `doc/md_files.txt` file will create revised `.rst` files, one for each entry listed in the registry. In summary:

```
cd doc
python ../dev/md_to_rst.py md_files.txt
```

### Refresh API Documentation

- Make sure dsgrid is installed or is in your PYTHONPATH
- Delete the contents of `api`.
- Run `sphinx-apidoc -o api ..` from the `doc` folder.
- Compare `api/modules.rst` to `api.rst`. Delete `setup.rst` and references to it.
- 'git push' changes to the documentation source code as needed.
- Make the documentation per below

### Building HTML Docs

Run `make html` for Mac and Linux; `make.bat html` for Windows.

### Pushing to GitHub Pages

**TODO:** Structure our GitHub Pages to preserve documentation for different
versions. Dheepak probably has suggestions.

#### Mac/Linux

```
make github
```

#### Windows

```
make.bat html
```

Then run the github-related commands by hand:

```
git branch -D gh-pages
git push origin --delete gh-pages
ghp-import -n -b gh-pages -m "Update documentation" ./_build/html
git checkout gh-pages
git push origin gh-pages
git checkout main # or whatever branch you were on
```

## Release on pypi

*Not yet available*
