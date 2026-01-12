# dsgrid Documentation

:::{admonition} Active Development
:class: warning

The dsgrid toolkit is under active development and details are subject to change. Please reach out to the [dsgrid team](mailto:dsgrid.info@nrel.gov) with questions or feedback.
:::

## What is dsgrid?

The **demand-side grid (dsgrid)** toolkit is a Python framework for compiling high-resolution energy demand datasets across multiple dimensions: time, geography, time, sector, subsector, enduse, etc. dsgrid enables researchers, analysts, and planners to integrate diverse energy datasets into cohesive projects suitable for power system analysis, policy evaluation, and energy planning.

For more information, please visit [https://www.nrel.gov/analysis/dsgrid.html](https://www.nrel.gov/analysis/dsgrid.html).

---

## Getting Started by Role

::::{grid} 2
:gutter: 3

:::{grid-item-card} Use Published Data
:link: getting_started/data_users
:link-type: doc

**For:** Data Users, Analysts, Researchers

Browse, download and analyze pre-compiled dsgrid datasets for your research or analysis.
:::

:::{grid-item-card} Submit a Dataset
:link: getting_started/dataset_submitters
:link-type: doc

**For:** Dataset Submitters, Modelers

Register and submit your energy demand dataset to a dsgrid project.
:::

:::{grid-item-card} Map a Dataset
:link: getting_started/dataset_mappers
:link-type: doc

**For:** Dataset Mappers, Data Engineers

Transform datasets to different time conventions, geographies, etc.
:::

:::{grid-item-card} Manage a Project
:link: getting_started/project_coordinators
:link-type: doc

**For:** Project Coordinators, Lead Analysts

Create projects, compile datasets, run queries, and create derived datasets.
:::

::::

---

## Key Features

- **Multi-Dimensional Data**: Organize energy data across scenario, geography, time, sector, subsector, enduse, and custom dimensions
- **Flexible Mappings**: Map datasets between different dimensional systems with explicit, documented transformations
- **Powerful Queries**: Aggregate, filter, and transform data across dimensions to create custom views
- **Big Data Support**: Process terabyte-scale datasets using Apache Spark or gigabyte-scale datasets using DuckDB
- **Data Integrity**: Validation ensures dataset consistency and project compatibility
- **Registry System**: Central metadata management for dimensions, datasets, and projects

---

## Quick Links for Stakeholders

If you're evaluating dsgrid for your organization:

- **Capabilities Overview**: See [dsgrid Fundamentals](user_guide/fundamentals.md) for a high-level introduction
- **Published Work**: Browse [Published Datasets](published_data/published_datasets.md) and [Publications](publications.md) to see dsgrid in action
- **Technical Team**: Direct your technical staff to [Getting Started](getting_started/index.md)
- **dsgrid Webpage**: Visit [https://www.nrel.gov/analysis/dsgrid.html](https://www.nrel.gov/analysis/dsgrid.html)

---

## Documentation Sections

### User Guide

- **[dsgrid Fundamentals](user_guide/fundamentals.md)** - Core concepts and architecture
- **[Tutorials](user_guide/tutorials/index.md)** - Step-by-step learning guides
- **[How-To Guides](user_guide/how_tos/index.md)** - Quick reference for specific tasks

### Topic Deep Dives

- **[Dataset Registration](user_guide/dataset_registration/index.md)** - Requirements, formats, and validation
- **[Dataset Mapping](user_guide/dataset_mapping/index.md)** - Dimension mappings and workflows
- **[Dataset Submittal](user_guide/dataset_submittal/index.md)** - Submission process and verification
- **[Project Creation](user_guide/project_creation/index.md)** - Building dsgrid projects
- **[Project Queries](user_guide/project_queries/index.md)** - Querying and analyzing projects
- **[Project Derived Datasets](user_guide/project_derived_datasets/index.md)** - Creating derived datasets

### Advanced Topics

- **[Apache Spark](user_guide/apache_spark/index.md)** - Working with large-scale datasets

### Software Reference

- **[CLI Reference](software_reference/cli_reference.md)** - Command-line interface documentation
- **[Software Architecture](software_reference/architecture.md)** - System design and components
- **[Data Models](software_reference/data_models/index.md)** - Configuration and data model specifications

### Additional Resources

- **[Publications](publications.md)** - Research papers and reports using dsgrid
- **[Citation & Attribution](citation.md)** - How to cite dsgrid and datasets
- **[Contact](contact.md)** - Get help or provide feedback

```{toctree}
:hidden:
:maxdepth: 2

getting_started/index
published_data/index
user_guide/index
software_reference/index
publications
citation
contact
```
