# Documentation

## Build

The documentation on github.com gets built automatically by a GitHub action. To build locally:

```
$ make html
```

or

```
> python -m sphinx -b html source _build/html
```

## Documentation Style Guide

### File format
Write all documentation pages in Markdown (.md)

### Line length
Do not exceed 99 characters on a line. Line breaks are not required but they
may it easier to read the raw text in editors and browsers.
