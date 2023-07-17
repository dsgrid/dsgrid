# Documentation

## Build

The documentation on github.com gets built automatically by a GitHub action. To build locally:
```
$ make html
```

## Documentation Style Guide

### File format
Write all documentation pages in reStructuredText (.rst). There are options to write content in
Markdown and then convert them to reStructuredText with pandoc, but that adds complexity.

### Line length
Do not exceed 99 characters on a line. Line breaks are not required by reStructuredText but they
may it easier to read the raw text in editors and browsers.

### Headings
Follow this [convention](https://devguide.python.org/documentation/markup/#sections) of heading
hierarchies:

```
# with overline, for parts
* with overline, for chapters
= for sections
- for subsections
^ for subsubsections
" for paragraphs
```

## Code blocks

### CLI commands
Define `console` code blocks whenever you include example CLI commands. Use `$` to denote the
prompt. The extension `sphinx_copybutton` allows the user to press a button to copy the text and
will exclude the prompt.

```
.. code-block:: console

    $ command --option args
```
