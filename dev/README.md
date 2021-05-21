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

In addition to grabbing the branch of dsgrid you want to test and making you've 
activated an environment with dsgrid installed per the above, you'll need to set 
up dsgrid for testing by setting environment variables:

```
# point to your checkout of the dsgrid-data-UnitedStates repository (adjusting
# the path as needed)
export US_DATA_REPO="/home/$USER/dsgrid-data-UnitedStates"
# feel free to use a different path for storing your test registry--this is just 
# an example
export DSGRID_REGISTRY_PATH="/home/$USER/.dsgrid-test-registry"
```

and then running the commands:
```
dsgrid-internal create-registry $DSGRID_REGISTRY_PATH
dsgrid registry --path=$DSGRID_REGISTRY_PATH --offline dimensions register $US_DATA_REPO/dsgrid_project/dimensions.toml -l "initial registration"
dsgrid registry --path=$DSGRID_REGISTRY_PATH --offline dimensions register $US_DATA_REPO/dsgrid_project/datasets/sector_models/comstock/dimensions.toml -l "initial registration"
# environment variable substitutions will not actually work--replace with paths
python -c 'from tests.common import *;replace_dimension_uuids_from_registry("${DSGRID_REGISTRY_PATH}", ["${US_DATA_REPO}/dsgrid_project/project.toml", "${US_DATA_REPO}/dsgrid_project/datasets/sector_models/comstock/dataset.toml"])'
dsgrid registry --path=$DSGRID_REGISTRY_PATH --offline projects register $US_DATA_REPO/dsgrid_project/project.toml -l "initial registration"
aws s3 sync s3://nrel-dsgrid-scratch/dsgrid_v2.0.0/commercial/load_data_lookup.parquet $DSGRID_REGISTRY_PATH/data/efs-comstock/1.0.0/load_data_lookup.parquet
aws s3 sync s3://nrel-dsgrid-scratch/dsgrid_v2.0.0/commercial/load_data.parquet $DSGRID_REGISTRY_PATH/data/efs-comstock/1.0.0/load_data.parquet
dsgrid registry --path=$DSGRID_REGISTRY_PATH --offline datasets register $US_DATA_REPO/dsgrid_project/datasets/sector_models/comstock/dataset.toml -l "initial registration"
dsgrid registry --path=$DSGRID_REGISTRY_PATH --offline projects submit-dataset -p test -d efs_comstock -l "initial registration"
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
