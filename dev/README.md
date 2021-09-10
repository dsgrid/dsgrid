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

## Tests

### Setup
The tests use the [test data repository](https://github.com/dsgrid/dsgrid-test-data.git)
as a git submodule in `./dsgrid-test-data`. It is a minimal version of the EFS project and
datasets. You must initialize this submodule and keep it updated.

Initialize the submodule:
```
git submodule init
git submodule update
```

Update the submodule when there are new changes in the test data repository:
```
git submodule update --remote --merge
```

### Run tests

To run all tests, including AWS tests:
```
pytest
```

If you want to exclude AWS tests:
```
pytest tests
```

If you only want to run AWS tests:
```
pytest tests_aws
```

### Workflow for developing a feature that changes code and data

If you are developing a feature that requires changing code and data then you will need to keep
the submodule synchronized. Here is an example workflow:

1. Create a dsgrid branch.
```
git checkout -b feature-x
```

2. Create a data branch inside the submodule.
```
cd dsgrid-test-data
git checkout -b feature-x
cd ..
```

3. Implement and test your feature.

4. Commit your code and data changes in each repository.

5. Update the dsgrid repo to point to the correct data commit.
```
git submodule set-branch -b feature-x dsgrid-test-data
git add .gitmodules dsgrid-test-data
git commit -m "Point dsgrid-test-data to feature-x branch"
```

6. Push both branches to GitHub. **Note**: Using a forked data repository is not supported.

7. Open two pull requests.

8. Address comments, if needed. If you make new commits to the data branch then you must update
the dsgrid branch before pushing back to GitHub.
```
git add dsgrid-test-data
git commit -m "Update dsgrid-test-data"
```

9. Merge the data pull request.

10. Update the dsgrid branch to point back to the `main` data branch.
```
cd dsgrid-test-data
git checkout main
git pull origin main
# The feature branch is now in main. Delete it.
git branch -d feature-x
cd ..
git submodule set-branch -b main dsgrid-test-data
git add .gitmodules dsgrid-test-data
git commit -m "Point dsgrid-test-data to main branch"
git push origin feature-x
```

11. After CI passes, merge the dsgrid pull request.

### Test registry
The setup code creates a local registry for testing in `./tests/data/registry`. This only happens
once in order to save time on repeated runs. If you make a change to code or data then you may
need to manually delete the directory.

### Pytest options

option flag           | effect
--------------------- | ------
--log-cli-level=debug | emits log messages to the console. level can be set to debug, info, warn, error


## Testing/exploring with the EFS project repository

You can create a local registry with the [EFS project repository](https://github.com/dsgrid/dsgrid-project-EFS)
and use it for testing and exploration.

Clone the repository to your system.
```
git clone https://github.com/dsgrid/dsgrid-project-EFS $HOME/dsgrid-project-EFS
```

Download the EFS datasets from AWS or Eagle (`/projects/dsgrid/efs_datasets/converted_output/commercial`)
to a local path and set an environment variable for it.

This is what that directory should contain:

```
tree ~/.dsgrid-data
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


Set environment variables to point to the registry and datasets.
```
export DSGRID_REGISTRY_PATH=./local-registry
export DSGRID_LOCAL_DATA_DIRECTORY=~/.dsgrid-data
```

Create and populate the registry.
```
python tests/make_us_data_registry.py $DSGRID_REGISTRY_PATH -p $HOME/dsgrid-project-EFS -d $DSGRID_LOCAL_DATA_DIRECTORY
```

Now you can run any `dsgrid registry` command.

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

### Sphinx Style Guide

1. Follow the heading hierarchy convention defined by
[Sphinx](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#sections).

2. Line length limit: 99 characters

3. Indentation: 4 spaces unless the text follows a bullet or continues a Sphinx directive.

```
# 4 spaces
::

    some raw text

# 4 spaces
.. code-block:: python

    import dsgrid

# 3 spaces
.. note:: some note
   continued

# 2 spaces
- a bullet description
  continued

```

### Markdown to reStructuredText

Markdown files are registered in `doc/md_files.txt`. Paths in that file should be relative to the docs folder and should exclude the file extension. For every file listed there, the `dev/md_to_rst.py` utility will expect to find a markdown (`.md`) file, and will look for an optional `.postfix` file, which is expected to contain `.rst` code to be appended to the `.rst` file created by converting the input `.md` file. Thus, running `dev/md_to_rst.py` on the `doc/md_files.txt` file will create revised `.rst` files, one for each entry listed in the registry. In summary:

```
cd doc
python ../dev/md_to_rst.py md_files.txt
```

### Refresh API Documentation

- Make sure dsgrid is installed or is in your PYTHONPATH
- Delete the contents of `source/api`.
- Run `sphinx-apidoc -o source/api ../dsgrid` from the `docs` folder.
- 'git push' changes to the documentation source code as needed.
- Make the documentation per below

### Building HTML Docs

From the `docs/` folder, run `make html` for Mac and Linux; `make.bat html` for Windows.

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

## Release on pypi

*Not yet available*
