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

From your local dsgrid repository:
```
pytest tests
```

If you want to include AWS tests:

```
pytest
```

The test setup code will clone [minimal test project and dataset](https://github.com/dsgrid/dsgrid-test-data.git)
to `./dsgrid-test-data`. It is a minimal version of the EFS project and dataset.

If you want to change the data branch being used, edit the constant `TEST_DATA_BRANCH` in
`tests/conftest.py`.

It will also create a local registry for testing in `./dsgrid-test-data/registry`.

The test code will not automatically update the data branch if is it already the correct branch.
You can update it manually or delete the directory and let the tests set it up again.

pytest options that may be helpful:

option flag           | effect
--------------------- | ------
--log-cli-level=DEBUG | emits log messages to the console. level can be set to DEBUG, INFO, WARN, ERROR


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

Now your can run any `dsgrid registry` command.

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
