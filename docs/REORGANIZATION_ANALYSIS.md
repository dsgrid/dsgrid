# Documentation Reorganization Analysis

## Current Structure

The dsgrid documentation currently follows the **Diátaxis framework** (tutorials, how-tos, explanations, reference), which is a solid foundation. Here's the current organization:

```
docs/
├── source/
│   ├── tutorials/           # 6 files - step-by-step guides with StandardScenarios example
│   ├── how_tos/            # 12 files - quick recipes for specific tasks
│   ├── explanations/       # 2 files + components/ subfolder (5 files)
│   ├── reference/          # 6 files + data_models/ + dsgrid_api/ subfolders
│   ├── index.rst           # Main landing page (very comprehensive, ~260 lines)
│   └── spark_overview.rst  # Standalone page (TODO: needs reorganization)
```

## Current Strengths

1. **Good framework adoption**: Diátaxis structure is industry best practice
2. **Rich landing page**: The `index.rst` provides excellent conceptual overview
3. **Role-based organization**: Clear delineation for Project Coordinators, Dataset Contributors, Data Users
4. **Component documentation**: Well-structured explanations of dimensions, datasets, projects, etc.

## Current Issues & Gaps

### 1. **Landing Page Overload**
- `index.rst` is 260+ lines and serves as both landing page AND comprehensive guide
- Contains detailed explanations that might belong in Explanations section
- Users need to scroll extensively to find navigation

### 2. **Orphaned Content**
- `spark_overview.rst` at root level (has TODO to reorganize)
- Some how-to guides commented out in index (create_project_base_dimensions, etc.)

### 3. **Incomplete Content**
- 20+ TODO markers throughout documentation
- Missing sections:
  - Queries explanation (just says "TODO")
  - Published Projects explanation (just says "TODO")
  - Standalone environment setup (just says "TODO")
  - NREL HPC environment setup (just says "TODO")

### 4. **Mixed Abstraction Levels**
- Reference section mixes high-level architecture with detailed API docs
- CLI fundamentals vs CLI reference could be better separated

### 5. **Tutorial Dependencies**
- Tutorials require external repo (dsgrid-project-StandardScenarios)
- Not clear if beginners should start with tutorials or how-tos
- Installation is in "How-To Guides" but should probably come first

## Proposed Reorganization Options

### Option 1: Enhanced Diátaxis (Recommended)

Keep the current framework but improve structure and reduce landing page weight:

```
docs/source/
├── index.rst                    # Streamlined landing page (focus on navigation)
├── introduction/                # NEW: Separate introduction section
│   ├── what_is_dsgrid.rst      # Overview from current index.rst
│   ├── key_concepts.rst        # Core concepts: dimensions, datasets, projects
│   ├── user_roles.rst          # Project coordinators, contributors, users
│   ├── getting_started.rst     # Quick start guide
│   └── architecture.rst        # Moved from reference/
├── tutorials/
│   ├── index.rst
│   ├── 01_setup_environment.rst        # NEW: Make setup explicit
│   ├── 02_create_project.rst
│   ├── 03_create_and_submit_dataset.rst
│   ├── 04_map_dataset.rst
│   ├── 05_query_project.rst
│   ├── 06_create_derived_dataset.rst
│   └── 07_query_dataset.rst
├── how_tos/
│   ├── index.rst
│   ├── installation/           # NEW: Group installation topics
│   │   ├── local_setup.rst
│   │   ├── kestrel_setup.rst
│   │   └── spark_cluster.rst
│   ├── registry/               # NEW: Group registry operations
│   │   └── browse_registry.rst
│   ├── dimensions/             # NEW: Group dimension tasks
│   │   ├── create_dataset_dimensions.rst
│   │   ├── create_project_base_dimensions.rst
│   │   └── create_project_supplemental_dimensions.rst
│   ├── queries/                # NEW: Group query tasks
│   │   ├── filter_a_query.rst
│   │   └── query_project.rst
│   ├── datasets/               # NEW: Group dataset tasks
│   │   └── create_derived_dataset.rst
│   └── integration/            # NEW: Group external tools
│       └── visualize_data_with_tableau.rst
├── explanations/
│   ├── index.rst
│   ├── components/             # Keep existing structure
│   │   ├── dimensions.rst
│   │   ├── datasets.rst
│   │   ├── projects.rst
│   │   ├── dimension_mappings.rst
│   │   └── derived_datasets.rst
│   ├── queries.rst             # Expand this (currently minimal)
│   ├── published_projects.rst  # NEW: Fill in TODO
│   ├── spark_and_duckdb.rst   # MOVED: from spark_overview.rst
│   └── computational_environments.rst  # NEW: Fill in TODO
├── reference/
│   ├── index.rst
│   ├── cli/                    # NEW: Group CLI reference
│   │   ├── fundamentals.rst
│   │   ├── commands.rst       # From cli.rst
│   │   └── examples.rst
│   ├── data_formats/           # NEW: Group data format specs
│   │   └── dataset_formats.rst
│   ├── data_models/            # Keep existing
│   │   └── [existing files]
│   ├── python_api/             # RENAMED from dsgrid_api/
│   │   └── [existing files]
│   └── glossary.rst            # NEW: Term definitions
└── contributing/               # NEW: Developer documentation
    ├── index.rst
    ├── development_setup.rst
    ├── testing.rst
    ├── code_style.rst
    └── documentation_guide.rst # Move from docs/README.md
```

**Benefits:**
- Reduces cognitive load on landing page
- Better content discovery through clear sections
- Numbered tutorials show progression
- Grouped how-tos are easier to navigate
- Introduction section serves as gentle onboarding

### Option 2: User Journey Focus

Organize by user workflow rather than documentation type:

```
docs/source/
├── index.rst
├── getting_started/
│   ├── introduction.rst
│   ├── installation.rst
│   ├── key_concepts.rst
│   └── quick_start.rst
├── for_dataset_contributors/
│   ├── creating_datasets.rst
│   ├── registering_datasets.rst
│   ├── submitting_to_projects.rst
│   └── reference/
├── for_project_coordinators/
│   ├── creating_projects.rst
│   ├── managing_dimensions.rst
│   ├── creating_derived_datasets.rst
│   ├── queries_and_analysis.rst
│   └── reference/
├── for_data_users/
│   ├── browsing_registry.rst
│   ├── writing_queries.rst
│   ├── accessing_data.rst
│   └── reference/
├── concepts/
│   └── [explanations content]
└── reference/
    └── [technical reference]
```

**Benefits:**
- Extremely clear for users knowing their role
- Self-contained sections for each persona

**Drawbacks:**
- Content duplication (queries appear in multiple sections)
- Harder to maintain consistency
- May create artificial boundaries

### Option 3: Progressive Disclosure

Organize by complexity level:

```
docs/source/
├── index.rst
├── level_1_essentials/
│   ├── what_is_dsgrid.rst
│   ├── installation.rst
│   ├── first_query.rst
│   └── browsing_data.rst
├── level_2_contributing/
│   ├── dataset_basics.rst
│   ├── creating_dimensions.rst
│   ├── registering_datasets.rst
│   └── submitting_datasets.rst
├── level_3_coordinating/
│   ├── project_design.rst
│   ├── dimension_mappings.rst
│   ├── derived_datasets.rst
│   └── publishing.rst
├── level_4_advanced/
│   ├── custom_queries.rst
│   ├── spark_optimization.rst
│   └── programmatic_api.rst
└── reference/
    └── [complete reference]
```

**Benefits:**
- Clear learning progression
- Prevents overwhelming beginners

**Drawbacks:**
- "Levels" may not match all user mental models
- Users may skip levels and get lost

## Recommendations

### Primary Recommendation: **Option 1 (Enhanced Diátaxis)**

This approach:
1. ✅ Preserves the well-understood Diátaxis framework
2. ✅ Addresses current issues (landing page overload, orphaned content)
3. ✅ Improves discoverability through better grouping
4. ✅ Maintains clear boundaries between doc types
5. ✅ Easier migration path from current structure

### Implementation Priority

**Phase 1: Critical Fixes (Quick Wins)**
1. Split index.rst into streamlined landing + introduction section
2. Move spark_overview.rst to proper location
3. Number tutorial files to show progression
4. Create installation/ subfolder in how_tos/

**Phase 2: Content Organization**
1. Group how-tos into logical subfolders
2. Expand incomplete explanations (queries, published projects)
3. Reorganize reference section with subfolders
4. Create glossary

**Phase 3: Content Development**
1. Fill in TODO sections
2. Create contributing/ section for developers
3. Add more examples and diagrams
4. Update cross-references

**Phase 4: Polish**
1. Review all internal links
2. Add navigation improvements (breadcrumbs, prev/next)
3. Improve search metadata
4. Add version-specific notes

## Quick Wins (Can Implement Immediately)

1. **Streamline index.rst**: Move detailed content to new introduction/ folder
2. **Number tutorials**: Rename files with numerical prefixes (01_, 02_, etc.)
3. **Move spark_overview.rst**: Place in explanations/spark_and_duckdb.rst
4. **Uncomment working how-tos**: Enable commented-out pages or remove if obsolete
5. **Create glossary.rst**: Define all domain terms in one place

## Considerations

### SEO & Discoverability
- Keep URLs stable during reorganization (use redirects)
- Maintain comprehensive cross-linking
- Add clear breadcrumbs

### User Testing
- Consider surveying current users on navigation pain points
- Track which pages are most visited
- Identify common search queries

### Maintenance
- Document the structure in docs/README.md
- Create templates for new pages
- Establish review process for new content

### Migration
- Create redirect map for any URL changes
- Update all internal references
- Update external links (README, etc.)

## Next Steps

1. **Decide on approach**: Choose Option 1, 2, 3, or hybrid
2. **Create migration plan**: Map current files to new structure
3. **Test reorganization**: Try restructure on a branch
4. **Review with team**: Get feedback from documentation users
5. **Implement in phases**: Don't try to do everything at once
6. **Update continuously**: Documentation is never "done"

## Questions to Consider

- Who are the primary users of the documentation?
- What are the most common documentation complaints?
- Which pages get the most traffic?
- Are there external dependencies on current URLs?
- What's the maintenance bandwidth for documentation?
- Should we add more visual aids (diagrams, videos)?
- Do we need multi-version documentation (coming releases)?
