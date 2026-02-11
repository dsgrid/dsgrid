# CLI Fundamentals

The dsgrid CLI tools contain some nuances that users should understand in order to have a good experience.

## General Usage

The dsgrid CLI commands are hierarchical with help at every level. For example:

```bash
$ dsgrid
$ dsgrid --help

$ dsgrid registry
$ dsgrid registry --help

$ dsgrid registry projects
$ dsgrid registry projects --help
$ dsgrid registry projects register --help

$ dsgrid query
$ dsgrid query --help
$ dsgrid query project run --help
```

### Registry CLI Commands

The `dsgrid registry` command group has a subgroup for each type of configuration stored in the registry: `projects`, `datasets`, `dimensions`, and `dimension-mappings`.

Each of those subgroups has four main commands:

- **`register`**: Register a new item (project, dataset, dimension, or dimension mapping). This command typically takes a JSON5 file as input. After registration, all other commands will refer to the item by its ID.
  - Projects and datasets have user-created string IDs (`project_id` and `dataset_id`), defined in the original JSON5 file
  - Dimensions and dimension mappings receive integer IDs from the registry during registration

- **`list`**: List items stored in the registry in a table. Each command offers the ability to filter the results by table columns.

- **`dump`**: Export items from the registry to local files. This command takes the item ID as input. Refer to the `list` commands to find the IDs.

- **`update`**: Change an existing item in the registry. A typical workflow would be to:
  1. Run the `dump` command to export a configuration to local files
  2. Edit the files
  3. Run the `update` command

Refer to each command's `--help` output for additional information. Full CLI documentation at [CLI Reference](../reference/cli).

## Shell Completion

The dsgrid CLI uses the Python package [Click](https://click.palletsprojects.com) to process CLI options and arguments. Click supports shell completion for commands and subcommands for Bash, Zsh, and Fish. **We highly recommend that you configure your shell for this.**

To demonstrate the value, let's suppose that you want to see the commands available. Type `dsgrid`, a space, and then `tab`. This is the result:

```bash
$ dsgrid <tab>
config             -- Config commands
download           -- Download a dataset.
install-notebooks  -- Install dsgrid notebooks to a local path.
query              -- Query group commands
registry           -- Manage a registry.
```

Press `tab` to cycle through the options. The same principle works for subcommands (e.g., `dsgrid registry <tab>`).

:::{important}
After running the steps below, **restart your shell** in order for the changes to take effect.
:::

### Bash Instructions

```bash
$ _DSGRID_COMPLETE=bash_source dsgrid > ~/.dsgrid-complete.bash
```

Add this line to your `~/.bashrc` file:

```bash
. ~/.dsgrid-complete.bash
```

### Zsh Instructions

```bash
$ _DSGRID_COMPLETE=zsh_source dsgrid > ~/.dsgrid-complete.zsh
```

Add this line to your `~/.zshrc` file:

```zsh
. ~/.dsgrid-complete.zsh
```

### Fish Instructions

```bash
$ _DSGRID_COMPLETE=fish_source dsgrid > ~/.config/fish/completions/dsgrid.fish
```

## Database Connection

All dsgrid commands require connecting to the database. We recommend that you use a dsgrid-provided shortcut to avoid having to type it in every command.

### dsgrid RC File

dsgrid allows you to store common configuration settings in a config file in your home directory. Here's how to create it with a database on the local computer. Change the hostname and database name as needed.

```bash
$ dsgrid config create sqlite:///<your-db-path> -N standard-scenarios
Wrote dsgrid config to /Users/username/.dsgrid.json5
```

This creates a configuration file that stores:
- Database connection URL
- Named connection profiles
- Default settings

**Example `.dsgrid.json5` file:**

```json5
{
  database_url: "sqlite:////scratch/username/dsgrid-registry/registry.db",
  connections: {
    "standard-scenarios": {
      database_url: "sqlite:////projects/dsgrid/standard-scenarios/registry.db"
    },
    "local": {
      database_url: "sqlite:////home/username/dsgrid-registry.db"
    }
  }
}
```

**Using named connections:**

```bash
# Use the default connection
$ dsgrid registry projects list

# Use a specific named connection
$ dsgrid -c standard-scenarios registry projects list

# Or use the long form
$ dsgrid --connection standard-scenarios registry projects list
```

### Environment Variables

You can also set these environment variables:

```bash
$ export DSGRID_REGISTRY_DATABASE_URL=sqlite:///<your-registry-path>
```

**Priority order** (highest to lowest):
1. Command-line `--database-url` option
2. Named connection via `-c/--connection`
3. `DSGRID_REGISTRY_DATABASE_URL` environment variable
4. Default connection in `.dsgrid.json5`

## Common CLI Patterns

### Filtering Lists

Most `list` commands support filtering:

```bash
# List all projects
$ dsgrid registry projects list

# Filter by project ID
$ dsgrid registry projects list --filter-config 'project_id=="StandardScenarios2021"'

# Filter datasets by submitter
$ dsgrid registry datasets list --filter-config 'submitter=="NREL"'
```

### Working with IDs

```bash
# Get dimension ID from list command
$ dsgrid registry dimensions list --filter-config 'dimension_type=="geography"'

# Use ID in other commands
$ dsgrid registry dimensions dump <dimension-id> -o ./dimensions
```

### Validation and Dry Run

Many commands support validation before execution:

```bash
# Validate configuration without registering
$ dsgrid registry projects register dataset_config.json5 --validate-only

# Show what would happen without making changes
$ dsgrid registry projects update project.json5 --dry-run
```

## Logging and Debugging

### Increase Log Verbosity

```bash
# Standard logging
$ dsgrid registry projects list

# Verbose logging (-v)
$ dsgrid -v registry projects list

# Very verbose logging (-vv)
$ dsgrid -vv registry projects list
```

### Log Files

dsgrid writes log files to:
- **Default location**: `./logs/dsgrid.log`
- **Custom location**: Set via `DSGRID_LOG_FILE` environment variable

```bash
$ export DSGRID_LOG_FILE=/path/to/my/logs/dsgrid.log
```

## Next Steps

- See the [CLI Reference](../reference/cli) for complete command documentation
- Learn about [installation](../getting_started/installation) and setup
- Understand the [software architecture](architecture)
- Follow the [tutorials](../user_guide/tutorials/index) for hands-on practice
