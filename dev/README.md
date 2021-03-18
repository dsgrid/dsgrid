# dsgrid Developer Readme

## Developer Dependencies

**pip extras**

```
pip install -e .[tests]

# or

pip install -e .[dev] # includes what is needed for tests and documentation

# or 

pip install -e .[admin] # dev plus what is needed for releasing packages
```

**Additional software required for publishing documentation:**

- [Pandoc](https://pandoc.org/installing.html)

## Run Tests


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
