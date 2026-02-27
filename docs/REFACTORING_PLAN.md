# dsgrid Documentation Refactoring Plan

## Overview

This plan outlines the refactoring of dsgrid documentation from RST to Markdown (with MyST parser), reorganizing content to be user-centric following the ComStock model.

## Goals

1. **User-Centric Navigation**: Help different user types quickly find relevant content
2. **Format Migration**: Convert from RST to Markdown using MyST parser
3. **Better Data Model Presentation**: Replace large Pydantic diagrams with clean tables
4. **Clear User Pathways**: Separate content for Data Users, Dataset Submitters, Dataset Mappers, and Project Coordinators

## Target User Types

1. **Clients/Funders** - High-level understanding, published outputs, where to direct technical staff
2. **Published Data Users** - Quick access to datasets with minimal friction
3. **Dataset Submitters** - Clear submission process, testing guidance, Spark guidance
4. **Dataset Mappers** - New use case for mapping datasets to different dimensions
5. **Project Coordinators** - Deep usage of all features including Spark, queries, derived datasets

## Documentation Structure Philosophy

### Tutorials vs. Detailed Descriptions

**Tutorials** (`user_guide/tutorials/`):
- Step-by-step walkthroughs for learning workflows
- All tutorials grouped in one location for easy discovery
- Follow Diátaxis-style tutorial approach: learning-oriented

**Detailed Descriptions** (topic sections):
- In-depth explanations of concepts, options, and processes
- Similar to the dataset_registration structure with multiple focused pages
- NOT repeating the Diátaxis pattern (tutorial/explanation/reference) in each section
- Instead: multiple detailed description pages covering different aspects

Example: `user_guide/dataset_registration/`
- `requirements.md` - Detailed description of dimension requirements
- `formats.md` - Detailed description of data formats
- `config_files.md` - Detailed description of configuration
- `validity_checks.md` - Detailed description of validation

This same pattern applies to:
- Dataset Mapping (dimension_mappings, mapping_types, workflows)
- Dataset Submittal (submission_process, verification, troubleshooting)
- Project Creation (project_concepts, base_dimensions, supplemental_dimensions, dataset_requirements)
- Project Queries (query_concepts, aggregations, filters, output_formats)
- Project Derived Datasets (derived_concepts, growth_rates, residuals, workflows)

### Why This Structure?

1. **Clear Tutorial Discovery**: All tutorials in one place makes learning path obvious
2. **Deep Dive Topics**: Detailed description pages allow thorough explanation without tutorial constraints
3. **No Artificial Separation**: Avoids forcing content into "tutorial/explanation/reference" when detailed descriptions make more sense
4. **Better Navigation**: Users can find related information grouped by topic, not by documentation type

## Proposed Structure

```
docs/source/
├── index.md                                    # NEW: User-centric landing page
├── getting_started/
│   ├── index.md
│   ├── data_users.md                          # NEW
│   ├── dataset_submitters.md                  # NEW
│   ├── dataset_mappers.md                     # NEW
│   └── project_coordinators.md                # NEW
├── published_data/
│   ├── index.md
│   ├── published_datasets.md                  # NEW
│   └── working_with_published_data/
│       ├── index.md
│       ├── prerequisites.md                   # NEW
│       └── tutorial.md                        # NEW
├── user_guide/
│   ├── index.md
│   ├── fundamentals.md                        # FROM: index.rst (concepts section)
│   ├── tutorials/
│   │   ├── index.md                           # FROM: tutorials/index.rst
│   │   ├── dataset_registration.md            # FROM: tutorials/create_and_submit_dataset.rst (registration part)
│   │   ├── dataset_submittal.md               # FROM: tutorials/create_and_submit_dataset.rst (submittal part)
│   │   ├── dataset_mapping.md                 # FROM: tutorials/map_dataset.rst
│   │   ├── dataset_query.md                   # FROM: tutorials/query_dataset.rst
│   │   ├── project_creation.md                # FROM: tutorials/create_project.rst
│   │   ├── project_query.md                   # FROM: tutorials/query_project.rst
│   │   └── derived_datasets.md                # FROM: tutorials/create_derived_dataset.rst
│   ├── how_tos/
│   │   └── index.md                           # FROM: how_tos/index.rst
│   ├── dataset_registration/
│   │   ├── index.md                           # NEW: Overview of dataset registration
│   │   ├── requirements.md                    # Detailed: Required dimensions, constraints
│   │   ├── formats.md                         # Detailed: One-table vs two-table formats
│   │   ├── config_files.md                    # Detailed: How to create config files
│   │   └── validity_checks.md                 # Detailed: Validation process and checks
│   ├── dataset_mapping/
│   │   ├── index.md                           # NEW: Overview of dataset mapping
│   │   ├── dimension_mappings.md              # Detailed: FROM: explanations/components/dimension_mappings.rst
│   │   ├── mapping_types.md                   # Detailed: Different mapping approaches
│   │   └── mapping_workflows.md               # Detailed: Step-by-step mapping process
│   ├── dataset_submittal/
│   │   ├── index.md                           # NEW: Overview of dataset submittal
│   │   ├── submission_process.md              # Detailed: Steps for submitting datasets
│   │   ├── verification.md                    # Detailed: Pre-submission verification
│   │   └── troubleshooting.md                 # Detailed: Common issues and solutions
│   ├── project_creation/
│   │   ├── index.md                           # NEW: Overview of project creation
│   │   ├── project_concepts.md                # Detailed: FROM: explanations/components/projects.rst
│   │   ├── base_dimensions.md                 # Detailed: Defining project base dimensions
│   │   ├── supplemental_dimensions.md         # Detailed: Adding supplemental dimensions
│   │   └── dataset_requirements.md            # Detailed: Specifying required datasets
│   ├── project_queries/
│   │   ├── index.md                           # NEW: Overview of project queries
│   │   ├── query_concepts.md                  # Detailed: FROM: explanations/queries.rst
│   │   ├── aggregations.md                    # Detailed: Aggregating across dimensions
│   │   ├── filters.md                         # Detailed: Filtering query results
│   │   └── output_formats.md                  # Detailed: Query output options
│   └── project_derived_datasets/
│       ├── index.md                           # NEW: Overview of derived datasets
│       ├── derived_concepts.md                # Detailed: FROM: explanations/components/derived_datasets.rst
│       ├── growth_rates.md                    # Detailed: Applying growth rates
│       ├── residuals.md                       # Detailed: Calculating residuals
│       └── workflows.md                       # Detailed: Creating derived datasets
├── apache_spark/
│   ├── index.md                               # FROM: spark_overview.rst
│   ├── when_to_use_spark.md                   # NEW
│   ├── kestrel_setup.md                       # FROM: how_tos/run_dsgrid_on_kestrel.rst
│   └── spark_cluster.md                       # FROM: how_tos/spark_cluster_on_kestrel.rst
├── software_reference/
│   ├── index.md
│   ├── cli_reference.md                       # FROM: reference/cli.rst
│   └── architecture.md                        # FROM: reference/architecture.rst
├── data_models/
│   ├── index.md                               # NEW: Table-based presentation
│   ├── dataset_model.md                       # FROM: reference/data_models/dataset.rst
│   ├── dimension_model.md                     # FROM: reference/data_models/dimension.rst
│   ├── dimension_mapping_model.md             # FROM: reference/data_models/dimension_mapping.rst
│   ├── project_model.md                       # FROM: reference/data_models/project.rst
│   └── enums.md                               # FROM: reference/data_models/enums.rst
├── publications.md                            # NEW
├── citation.md                                # NEW
└── contact.md                                 # NEW
```

## Key Changes

### 1. Format Migration (RST → Markdown)

**Technical Requirements:**
- Add `myst_parser` to Sphinx extensions
- Configure MyST extensions for advanced features
- Support both `.rst` and `.md` during transition
- Update `conf.py` with MyST configuration

**Files to Convert:**
- All tutorial files
- All how-to guides
- All explanation pages
- Reference pages (except auto-generated API docs)

### 2. Data Models Presentation

**Current Problem:**
- Large, unwieldy Pydantic model diagrams
- Difficult to install diagram generation tools
- Shows Pydantic internals

**Proposed Solution:**
- Use Markdown tables instead of diagrams
- One table per model showing:
  - Field name (with type)
  - Description
  - Required/Optional
  - Default value (if any)
  - Link to related models
- Hide Pydantic internal fields
- Add examples for each model

**Example Table Format:**
```markdown
## DatasetConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| dataset_id | str | Yes | Unique identifier for the dataset |
| dimensions | [DimensionsListModel](#dimensionslistmodel) | Yes | Dataset dimensions |
| version | str | Yes | Dataset version following semver |
| data_schema | DataSchemaModel | Yes | Schema defining data format |
```

### 3. New Content to Create

#### High Priority
1. **Getting Started guides** for each user type (data_users, dataset_submitters, dataset_mappers, project_coordinators)
2. **Published Datasets page** with links and descriptions
3. **Working with Published Data tutorial** for data users
4. **Dataset Mapping detailed descriptions** (new use case - mapping_types, workflows)
5. **Dataset Submittal detailed descriptions** (submission_process, verification, troubleshooting)
6. **Apache Spark guidance** on when to use it
7. **Publications page**
8. **Citation page**
9. **Contact page**

#### Medium Priority
1. **Index pages** for each major section
2. **Project Creation detailed descriptions** (base_dimensions, supplemental_dimensions, dataset_requirements)
3. **Project Queries detailed descriptions** (aggregations, output_formats)
4. **Project Derived Datasets detailed descriptions** (growth_rates, residuals, workflows)

#### Content Sourcing
Many "new" detailed description pages will be created by:
- Extracting and expanding content from existing tutorials
- Breaking down large explanation files into focused topics
- Adding practical guidance from how-to guides

### 4. Content to Migrate

**All Tutorials → `user_guide/tutorials/`:**
- `tutorials/create_and_submit_dataset.rst` splits into:
  - `user_guide/tutorials/dataset_registration.md` (registration part)
  - `user_guide/tutorials/dataset_submittal.md` (submittal part)
- `tutorials/map_dataset.rst` → `user_guide/tutorials/dataset_mapping.md`
- `tutorials/query_dataset.rst` → `user_guide/tutorials/dataset_query.md`
- `tutorials/create_project.rst` → `user_guide/tutorials/project_creation.md`
- `tutorials/query_project.rst` → `user_guide/tutorials/project_query.md`
- `tutorials/create_derived_dataset.rst` → `user_guide/tutorials/derived_datasets.md`

**Dataset Registration (Detailed Descriptions):**
- `explanations/components/dimensions.rst` → `user_guide/dataset_registration/requirements.md`
- `reference/dataset_formats.rst` → `user_guide/dataset_registration/formats.md`
- Tutorial content → `user_guide/dataset_registration/config_files.md`
- New content → `user_guide/dataset_registration/validity_checks.md`

**Dataset Mapping (Detailed Descriptions):**
- `explanations/components/dimension_mappings.rst` → `user_guide/dataset_mapping/dimension_mappings.md`
- New content → `user_guide/dataset_mapping/mapping_types.md`
- New content → `user_guide/dataset_mapping/mapping_workflows.md`

**Dataset Submittal (Detailed Descriptions):**
- Tutorial content → detailed pages in `user_guide/dataset_submittal/`
- New content → `user_guide/dataset_submittal/submission_process.md`
- New content → `user_guide/dataset_submittal/verification.md`
- New content → `user_guide/dataset_submittal/troubleshooting.md`

**Project Creation (Detailed Descriptions):**
- `explanations/components/projects.rst` → `user_guide/project_creation/project_concepts.md`
- New content → `user_guide/project_creation/base_dimensions.md`
- New content → `user_guide/project_creation/supplemental_dimensions.md`
- New content → `user_guide/project_creation/dataset_requirements.md`

**Project Queries (Detailed Descriptions):**
- `explanations/queries.rst` → `user_guide/project_queries/query_concepts.md`
- `how_tos/filter_a_query.rst` → `user_guide/project_queries/filters.md`
- New content → `user_guide/project_queries/aggregations.md`
- New content → `user_guide/project_queries/output_formats.md`

**Project Derived Datasets (Detailed Descriptions):**
- `explanations/components/derived_datasets.rst` → `user_guide/project_derived_datasets/derived_concepts.md`
- New content → `user_guide/project_derived_datasets/growth_rates.md`
- New content → `user_guide/project_derived_datasets/residuals.md`
- New content → `user_guide/project_derived_datasets/workflows.md`

**Other Migrations:**
- `how_tos/*.rst` → migrate useful content into appropriate sections
- `spark_overview.rst` → `apache_spark/index.md`
- `how_tos/run_dsgrid_on_kestrel.rst` → `apache_spark/kestrel_setup.md`
- `how_tos/spark_cluster_on_kestrel.rst` → `apache_spark/spark_cluster.md`
- `reference/cli.rst` → `software_reference/cli_reference.md`
- `reference/architecture.rst` → `software_reference/architecture.md`
- `index.rst` splits into:
  - `index.md` (user-centric landing page)
  - `user_guide/fundamentals.md` (concepts)
  - `getting_started/*.md` (user pathways)

**Data Models (Convert to Tables):**
- `reference/data_models/dataset.rst` → `data_models/dataset_model.md`
- `reference/data_models/dimension.rst` → `data_models/dimension_model.md`
- `reference/data_models/dimension_mapping.rst` → `data_models/dimension_mapping_model.md`
- `reference/data_models/project.rst` → `data_models/project_model.md`
- `reference/data_models/enums.rst` → `data_models/enums.md`

## Implementation Status

**Last Updated:** December 5, 2024
**Current Status:** Core migration complete, awaiting team review before proceeding

### Completed Work

✅ **Infrastructure & Setup**
- MyST parser configured in conf.py with all extensions
- New user-centric directory structure created
- sphinx-design added for card-based landing page
- Both .rst and .md file support working

✅ **Content Migration - Phases A-D Complete**
- All 6 explanation files converted and distributed to topic sections
- All dataset format documentation migrated (with eval-rst includes for data models)
- All 5 centralized tutorials converted to Markdown
- All 9 how-to guides converted and distributed to topic sections

✅ **Apache Spark Section (Task 3)**
- Migrated spark_overview.rst → apache_spark/overview.md (610 lines)
- Comprehensive coverage: Windows setup, cluster modes, running applications, configuration, troubleshooting

✅ **Link Updates (Task 2)**
- Updated all internal documentation links
- Removed references to old Diataxis directories (explanations/, tutorials/, how_tos/)
- Added legacy directory notes where appropriate

✅ **Cleanup (Task 7)**
- Removed old directories: explanations/, tutorials/, how_tos/
- Kept reference/ directory for auto-generated CLI/API docs

✅ **Build System**
- Documentation builds successfully
- Temporarily disabled sphinx.ext.autosectionlabel due to NoneType parent error
- Build completes with 340 warnings (mostly pre-existing duplicate labels)
- HTML output at _build/html/index.html

✅ **Navigation Improvements**
- Fixed installation page TOC issue by removing from getting_started toctree
- Added installation prerequisite links to data_users, dataset_submitters, project_coordinators pages
- Improved enum reference page to display enum values directly (added :undoc-members: and :member-order: bysource)

### Current State: Ready for Review

The documentation structure has been successfully refactored to be user-centric with clear navigation paths. The build system is working, and all major content has been migrated from the old Diataxis structure.

**What's Working:**
- User-centric landing page with role-based cards
- Clear topic sections (dataset_registration, dataset_mapping, project_creation, etc.)
- Centralized tutorials section
- Apache Spark guidance integrated
- Data models with improved enum display
- Mixed RST/Markdown support during transition

**Known Issues:**
- sphinx.ext.autosectionlabel temporarily disabled (NoneType parent error - needs investigation)
- 340 build warnings (mostly duplicate labels in auto-generated content - pre-existing)
- Getting Started pages are stubs (intentionally - awaiting content)

**Next Phase: Content Population (Awaiting Team Review)**

Before proceeding with populating the stub pages, team review is recommended to validate:
1. New navigation structure meets user needs
2. Topic organization is intuitive
3. Content distribution makes sense
4. Any adjustments needed before writing new content

## Implementation Phases

### Phase 0: Preparation ✅ COMPLETE
- [x] Create refactoring plan
- [x] Review and approve plan with team
- [x] Create backup branch
- [x] Update `conf.py` for MyST support
- [x] Test basic MyST build

### Phase 1: Infrastructure ✅ COMPLETE
- [x] Create new directory structure
- [x] Create index pages for all major sections
- [x] Update `conf.py` completely
- [x] Test documentation builds

### Phase 2: Landing & Getting Started ✅ COMPLETE (Structure)
- [x] Create new `index.md` (user-centric landing page with cards)
- [x] Create `getting_started/data_users.md` (stub with installation link)
- [x] Create `getting_started/dataset_submitters.md` (stub with installation link)
- [x] Create `getting_started/dataset_mappers.md` (stub)
- [x] Create `getting_started/project_coordinators.md` (stub with installation link)
- [x] Create `user_guide/fundamentals.md` (from current index.rst)
- [ ] **PENDING:** Populate getting started content (awaiting team review)

### Phase 3: Published Data Section ⏸️ PAUSED
- [x] Create `published_data/published_datasets.md` (stub)
- [x] Create `published_data/working_with_published_data/prerequisites.md` (stub)
- [ ] **PENDING:** Populate published data content (awaiting team review)

### Phase 4: Tutorials Section ✅ COMPLETE
- [x] Create `user_guide/tutorials/index.md`
- [x] Convert all 5 centralized tutorials to Markdown
- [x] Update all internal links

### Phase 5: Dataset Registration ✅ COMPLETE
- [x] Create `user_guide/dataset_registration/index.md`
- [x] Convert `explanations/components/dimensions.rst` → `requirements.md`
- [x] Migrate `reference/dataset_formats.rst` with eval-rst includes
- [x] Integrate data model tables via eval-rst

### Phase 6: Dataset Mapping ✅ COMPLETE
- [x] Create `user_guide/dataset_mapping/index.md`
- [x] Convert `explanations/components/dimension_mappings.rst` → `dimension_mappings.md`
- [x] Distribute how-to guide content

### Phase 7: Project Creation ✅ COMPLETE
- [x] Create `user_guide/project_creation/index.md`
- [x] Convert `explanations/components/projects.rst` → `project_concepts.md`
- [x] Distribute how-to guide content

### Phase 8: Project Queries ✅ COMPLETE
- [x] Create `user_guide/project_queries/index.md`
- [x] Convert `explanations/queries.rst` → `query_concepts.md`
- [x] Convert all query-related how-to guides
- [x] Distribute filter, visualization, and output content

### Phase 9: Project Derived Datasets ✅ COMPLETE
- [x] Create `user_guide/project_derived_datasets/index.md`
- [x] Convert `explanations/components/derived_datasets.rst` → `derived_concepts.md`

### Phase 10: Apache Spark ✅ COMPLETE
- [x] Convert `spark_overview.rst` → `apache_spark/overview.md` (610 lines)
- [x] Convert `how_tos/run_dsgrid_on_kestrel.rst` → `kestrel_setup.md`
- [x] Convert `how_tos/spark_cluster_on_kestrel.rst` → `spark_cluster.md`
- [x] Update apache_spark/index.md

### Phase 11: Software Reference ✅ COMPLETE
- [x] Software reference structure maintained
- [x] Data models with improved enum display
- [x] CLI and API reference preserved as RST (auto-generated)

### Phase 12: Cleanup ✅ COMPLETE
- [x] Remove old directories (explanations/, tutorials/, how_tos/)
- [x] Update all internal links
- [x] Fix navigation issues (installation TOC problem)
- [x] Test documentation build
- [x] Improve enum reference display

### Phase 13: Additional Content ⏸️ PAUSED (Awaiting Team Review)
- [ ] Populate `getting_started/*.md` pages
- [ ] Populate `published_data/*.md` pages
- [ ] Create `publications.md`
- [ ] Create `citation.md`
- [ ] Create `contact.md`

### Phase 14: Final Testing & Polish ⏸️ PAUSED
- [ ] Test all internal links systematically
- [ ] Test all navigation paths
- [ ] Verify all conversions
- [ ] Address remaining build warnings
- [ ] Investigate autosectionlabel issue
- [ ] Get team feedback
- [ ] Make final corrections

## Technical Considerations

### MyST Parser Configuration

```python
# conf.py additions
extensions = [
    "myst_parser",
    # ... existing extensions
]

myst_enable_extensions = [
    "colon_fence",      # ::: syntax
    "deflist",          # Definition lists
    "fieldlist",        # Field lists
    "html_admonition",  # Admonitions
    "html_image",       # Advanced image syntax
    "linkify",          # Auto-link URLs
    "replacements",     # Text replacements
    "smartquotes",      # Smart quotes
    "substitution",     # Variable substitution
    "tasklist",         # Task lists
]

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}
```

### Data Model Table Template

```markdown
## ModelName

Brief description of the model.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| field_name | str | Yes | - | Field description with [linked model](#other-model) if applicable |
| optional_field | int | No | 0 | Optional field description |

### Example

\`\`\`json
{
  "field_name": "example_value",
  "optional_field": 42
}
\`\`\`
```

### URL Redirects

Create a redirect map for changed URLs:
- `/tutorials/create_and_submit_dataset.html` → `/user_guide/dataset_registration/tutorial.html`
- `/tutorials/map_dataset.html` → `/user_guide/dataset_mapping/tutorial.html`
- etc.

## Success Criteria

- [ ] All documentation builds without errors
- [ ] All internal links work
- [ ] Each user type can find their pathway quickly (< 2 clicks from landing page)
- [ ] Data models are presented in clear tables with examples
- [ ] No Pydantic internals visible in documentation
- [ ] MyST Markdown used throughout (except auto-generated content)
- [ ] Navigation is intuitive for all user types
- [ ] Apache Spark guidance is clear about when it's needed

## Risks & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing URLs | High | Implement redirects, maintain old structure temporarily |
| MyST parser issues | Medium | Test early, have fallback plan |
| Content gaps during migration | Medium | Migrate in phases, keep old docs accessible |
| Data model conversion complexity | Medium | Start with one model as template |
| Team bandwidth | High | Extend timeline if needed, prioritize phases |

## Questions to Resolve

1. Should we maintain old RST files during transition?
2. What's the deployment timeline expectation?
3. Who will review each phase?
4. Do we need to coordinate with any external documentation links?
5. Should we create a "What's New" page explaining the reorganization?

## Next Steps

1. Review and approve this plan
2. Make any necessary adjustments
3. Begin Phase 0 (Preparation)
4. Set up regular check-ins during implementation
