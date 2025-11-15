# Documentation Migration Plan

## New Structure Overview

```
docs/source/
├── index.rst                           # NEW: User-centric landing page
├── getting_started.rst                 # NEW: Quick overview
├── fundamentals.rst                    # NEW: Core dsgrid concepts
├── published_data/
│   ├── index.rst                       # NEW
│   ├── published_datasets.rst          # NEW
│   └── working_with_published_data/
│       ├── index.rst
│       ├── prerequisites.rst           # NEW
│       └── tutorial.rst                # NEW
├── dataset/
│   ├── index.rst                       # NEW: Overview of dataset section
│   ├── registration/
│   │   ├── index.rst
│   │   ├── prerequisites.rst           # NEW
│   │   ├── tutorial.rst                # FROM: tutorials/create_and_submit_dataset.rst (partial)
│   │   ├── explanations/
│   │   │   ├── index.rst
│   │   │   ├── required_dimensions.rst # FROM: explanations/components/dimensions.rst (partial)
│   │   │   ├── dataset_formats.rst     # FROM: reference/dataset_formats.rst
│   │   │   ├── creating_configs.rst    # FROM: tutorials/create_and_submit_dataset.rst (partial)
│   │   │   └── validity_checking.rst   # NEW
│   │   └── reference/
│   │       ├── index.rst
│   │       └── dataset_config.rst      # FROM: reference/data_models/dataset.rst
│   ├── mapping_and_query/
│   │   ├── index.rst
│   │   ├── tutorial/
│   │   │   ├── index.rst
│   │   │   ├── querying.rst            # FROM: tutorials/query_dataset.rst
│   │   │   └── mapping.rst             # FROM: tutorials/map_dataset.rst
│   │   ├── explanations/
│   │   │   ├── index.rst
│   │   │   ├── dimension_mappings.rst  # FROM: explanations/components/dimension_mappings.rst
│   │   │   └── queries.rst             # FROM: explanations/queries.rst
│   │   └── reference/
│   │       ├── index.rst
│   │       └── mapping_config.rst      # FROM: reference/data_models/dimension_mapping.rst
│   └── submittal/
│       ├── index.rst
│       ├── tutorial.rst                # FROM: tutorials/create_and_submit_dataset.rst (submittal part)
│       ├── explanations/
│       │   └── index.rst
│       └── reference/
│           └── index.rst
├── projects/
│   ├── index.rst                       # NEW: Project section overview
│   ├── tutorial/
│   │   ├── index.rst
│   │   ├── create_project.rst          # FROM: tutorials/create_project.rst
│   │   ├── query_project.rst           # FROM: tutorials/query_project.rst
│   │   └── create_derived_dataset.rst  # FROM: tutorials/create_derived_dataset.rst
│   ├── explanations/
│   │   ├── index.rst
│   │   ├── projects.rst                # FROM: explanations/components/projects.rst
│   │   ├── derived_datasets.rst        # FROM: explanations/components/derived_datasets.rst
│   │   └── queries.rst                 # FROM: explanations/queries.rst
│   └── reference/
│       ├── index.rst
│       ├── project_repositories.rst    # NEW: Links to current/completed projects
│       ├── publications.rst            # NEW: Project-related publications
│       └── project_config.rst          # FROM: reference/data_models/project.rst
├── software_reference/
│   ├── index.rst
│   ├── cli/
│   │   ├── index.rst
│   │   ├── fundamentals.rst            # FROM: reference/cli_fundamentals.rst
│   │   └── commands.rst                # FROM: reference/cli.rst
│   ├── architecture.rst                # FROM: reference/architecture.rst
│   ├── data_models/
│   │   └── [keep existing structure]   # FROM: reference/data_models/*
│   └── apache_spark/
│       ├── index.rst
│       ├── overview.rst                # FROM: spark_overview.rst
│       └── kestrel_setup/
│           ├── running_dsgrid.rst      # FROM: how_tos/run_dsgrid_on_kestrel.rst
│           └── spark_cluster.rst       # FROM: how_tos/spark_cluster_on_kestrel.rst
├── publications.rst                    # NEW
├── citation.rst                        # NEW
└── contact.rst                         # NEW
```

## Detailed File Mapping

### Files to CREATE (New Content Needed)

| New File | Purpose | Priority |
|----------|---------|----------|
| `index.rst` | User-centric landing page inspired by ComStock | HIGH |
| `getting_started.rst` | Quick overview for all user types | HIGH |
| `fundamentals.rst` | Core dsgrid concepts | HIGH |
| `published_data/published_datasets.rst` | List/describe published datasets | HIGH |
| `published_data/working_with_published_data/prerequisites.rst` | Setup for data users | HIGH |
| `published_data/working_with_published_data/tutorial.rst` | Quick start for grabbing data | HIGH |
| `dataset/registration/prerequisites.rst` | Setup for dataset submitters | MEDIUM |
| `dataset/registration/explanations/validity_checking.rst` | How to validate datasets | MEDIUM |
| `dataset/submittal/tutorial.rst` | Submittal process | HIGH |
| `projects/reference/project_repositories.rst` | Links to StandardScenarios, etc. | MEDIUM |
| `publications.rst` | dsgrid publications | LOW |
| `citation.rst` | How to cite dsgrid | MEDIUM |
| `contact.rst` | How to contact team | LOW |

### Files to MOVE (Direct Migration)

| Current Location | New Location | Changes Needed |
|------------------|--------------|----------------|
| `tutorials/create_and_submit_dataset.rst` | Split into `dataset/registration/tutorial.rst` + `dataset/submittal/tutorial.rst` | Split content |
| `tutorials/map_dataset.rst` | `dataset/mapping_and_query/tutorial/mapping.rst` | Minimal |
| `tutorials/query_dataset.rst` | `dataset/mapping_and_query/tutorial/querying.rst` | Minimal |
| `tutorials/create_project.rst` | `projects/tutorial/create_project.rst` | Minimal |
| `tutorials/query_project.rst` | `projects/tutorial/query_project.rst` | Minimal |
| `tutorials/create_derived_dataset.rst` | `projects/tutorial/create_derived_dataset.rst` | Minimal |
| `reference/dataset_formats.rst` | `dataset/registration/explanations/dataset_formats.rst` | Minimal |
| `reference/cli_fundamentals.rst` | `software_reference/cli/fundamentals.rst` | Minimal |
| `reference/cli.rst` | `software_reference/cli/commands.rst` | Minimal |
| `reference/architecture.rst` | `software_reference/architecture.rst` | Minimal |
| `spark_overview.rst` | `software_reference/apache_spark/overview.rst` | Minimal |
| `how_tos/run_dsgrid_on_kestrel.rst` | `software_reference/apache_spark/kestrel_setup/running_dsgrid.rst` | Minimal |
| `how_tos/spark_cluster_on_kestrel.rst` | `software_reference/apache_spark/kestrel_setup/spark_cluster.rst` | Minimal |

### Files to EXTRACT (Split Existing Content)

| Source File | Extract To | What to Extract |
|-------------|------------|-----------------|
| `explanations/components/dimensions.rst` | `dataset/registration/explanations/required_dimensions.rst` | Dataset dimension requirements |
| `explanations/components/dimension_mappings.rst` | `dataset/mapping_and_query/explanations/dimension_mappings.rst` | Mapping concepts |
| `explanations/components/projects.rst` | `projects/explanations/projects.rst` | Project concepts |
| `explanations/components/derived_datasets.rst` | `projects/explanations/derived_datasets.rst` | Derived dataset concepts |
| `explanations/queries.rst` | Split to `dataset/mapping_and_query/explanations/queries.rst` + `projects/explanations/queries.rst` | Query concepts for each context |
| `index.rst` (lines 20-180) | `fundamentals.rst` | Core concepts section |

### Files to KEEP (Reference Material)

These files stay in their current location under `reference/data_models/`:
- `reference/data_models/dataset.rst` → becomes `dataset/registration/reference/dataset_config.rst`
- `reference/data_models/dimension.rst` → stays in `software_reference/data_models/`
- `reference/data_models/dimension_mapping.rst` → becomes `dataset/mapping_and_query/reference/mapping_config.rst`
- `reference/data_models/project.rst` → becomes `projects/reference/project_config.rst`
- `reference/data_models/enums.rst` → stays in `software_reference/data_models/`
- All API docs in `reference/dsgrid_api/` → move to `software_reference/data_models/`

### Files to DEPRECATE (May Delete or Archive)

| Current File | Reason | Action |
|--------------|--------|--------|
| `how_tos/installation.rst` | Move key parts to prerequisites in each section | Extract then archive |
| `how_tos/browse_registry.rst` | Integrate into published_data tutorial | Extract then archive |
| `how_tos/create_dataset_dimensions.rst` | Integrate into dataset/registration/tutorial | Extract then archive |
| `how_tos/filter_a_query.rst` | Integrate into query tutorials | Extract then archive |
| `how_tos/visualize_data_with_tableau.rst` | Could move to published_data or archive | Keep as supplemental |
| `how_tos/create_project_base_dimensions.rst` | Commented out - integrate or delete | Review first |
| `how_tos/create_project_supplemental_dimensions.rst` | Commented out - integrate or delete | Review first |

## Implementation Phases

### Phase 1: Foundation (Week 1)
**Goal:** New landing page and user pathways

1. ✅ Create new `index.rst` with user-centric approach
2. ✅ Create `getting_started.rst`
3. ✅ Create `fundamentals.rst` (extract from current index.rst)
4. ✅ Create directory structure for new organization
5. ✅ Update `conf.py` if needed

### Phase 2: Published Data Path (Week 2)
**Goal:** Serve most numerous users (Data Users)

1. ✅ Create `published_data/` section
2. ✅ Create prerequisites and tutorial for data users
3. ✅ Extract relevant content from `how_tos/browse_registry.rst`
4. ✅ Test navigation flow

### Phase 3: Dataset Registration Path (Week 3)
**Goal:** Support Dataset Submitters

1. ✅ Create `dataset/registration/` section
2. ✅ Split `tutorials/create_and_submit_dataset.rst`
3. ✅ Move `reference/dataset_formats.rst`
4. ✅ Create explanations subfolder with content
5. ✅ Create prerequisites

### Phase 4: Dataset Operations (Week 4)
**Goal:** Complete dataset section

1. ✅ Create `dataset/mapping_and_query/` section
2. ✅ Move mapping and query tutorials
3. ✅ Extract and organize explanations
4. ✅ Create `dataset/submittal/` section

### Phase 5: Projects Path (Week 5)
**Goal:** Support Project Coordinators

1. ✅ Create `projects/` section
2. ✅ Move project tutorials
3. ✅ Organize explanations
4. ✅ Create reference with repository links

### Phase 6: Software Reference (Week 6)
**Goal:** Consolidate technical reference

1. ✅ Create `software_reference/` section
2. ✅ Move CLI documentation
3. ✅ Move architecture and data models
4. ✅ Organize Apache Spark content

### Phase 7: Polish & Cleanup (Week 7)
**Goal:** Final touches

1. ✅ Create publications, citation, contact pages
2. ✅ Update all cross-references
3. ✅ Remove old files
4. ✅ Update navigation (toctrees)
5. ✅ Test all links
6. ✅ Build and review full site

### Phase 8: Review & Launch (Week 8)
**Goal:** Team review and deployment

1. ✅ Internal review with team
2. ✅ User testing with representatives from each role
3. ✅ Address feedback
4. ✅ Deploy

## Cross-Reference Updates Needed

After moving files, these cross-references will need updating:

1. All `:ref:` links in RST files
2. All `.. toctree::` directives
3. Links from README.md
4. Links from `.github/copilot-instructions.md`
5. Any hardcoded URLs in notebooks or examples

## Testing Checklist

- [ ] All pages build without errors
- [ ] All internal links work
- [ ] All code examples still valid
- [ ] Navigation breadcrumbs correct
- [ ] Search functionality works
- [ ] Mobile/responsive view works
- [ ] Each user type can find their path quickly

## URL Redirect Map

If documentation is already published, create redirects:

```
/tutorials/create_and_submit_dataset.html → /dataset/registration/tutorial.html
/tutorials/map_dataset.html → /dataset/mapping_and_query/tutorial/mapping.html
/tutorials/query_dataset.html → /dataset/mapping_and_query/tutorial/querying.html
/tutorials/create_project.html → /projects/tutorial/create_project.html
/tutorials/query_project.html → /projects/tutorial/query_project.html
/tutorials/create_derived_dataset.html → /projects/tutorial/create_derived_dataset.html
/reference/cli.html → /software_reference/cli/commands.html
/spark_overview.html → /software_reference/apache_spark/overview.html
```

## Notes

- Keep backups of original structure in a branch
- Consider maintaining old structure temporarily with deprecation notices
- Update any external documentation that links to these pages
- Monitor 404s after deployment to catch missed redirects
