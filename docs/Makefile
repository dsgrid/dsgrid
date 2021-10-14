# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
    python ..\dev\md_to_rst.py $(SOURCEDIR)/md_files.txt
	rm -rf $(SOURCEDIR)/api
    sphinx-apidoc -o $(SOURCEDIR)/api ../dsgrid
	python make_config_model_rst.py -o $(BUILDDIR)/config_rsts
	python make_example_configs.py
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# TODO: Delete? Publishing to gh-pages happens on push to main
# github: html
#	-git branch -D gh-pages
#	-git push origin --delete gh-pages
#	ghp-import -n -b gh-pages -m "Update documentation" ./_build/html
#	git checkout gh-pages
#	git push --set-upstream origin gh-pages
#	git checkout ${BRANCH}