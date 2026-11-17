# How to Update the Simple-Standard-Scenarios Test Registry

The tests use a filtered version of the StandardScenarios project stored in the
[dsgrid-test-data](https://github.com/dsgrid/dsgrid-test-data) submodule at
`dsgrid-test-data/filtered_registries/simple_standard_scenarios`. If you update the configs or data
for the StandardScenarios project, you need to regenerate this registry with the steps below.

These steps require Apache Spark and access to NLR's Kestrel HPC. Refer to
[How to Start a Spark Cluster on Kestrel](../../user_guide/how_tos/spark_cluster_on_kestrel).

:::{note}
These steps are run infrequently. If a command no longer matches the current CLI, check its
`--help` output and please update this page.
:::

## Steps

1. **Register and submit all datasets to a clean registry.** Three compute nodes on Kestrel are
   recommended in order to complete the job in one hour. Two may be sufficient if you run multiple
   iterations. Run these commands from your scratch directory, after modifying the paths specified
   in `tests/data/standard_scenarios_registration.json5` as needed.

   Create the registry:

   ```bash
   dsgrid create-registry sqlite:///./standard-scenarios.db -p standard-scenarios-registry-data
   ```

   Register and submit the project and datasets:

   ```bash
   spark-submit \
       --master=spark://$(hostname):7077 \
       --conf spark.sql.shuffle.partitions=2400 \
       $(which dsgrid-cli.py) --url sqlite:///./standard-scenarios.db registry bulk-register \
           --data-base-dir=standard-scenarios-registry-data \
           tests/data/standard_scenarios_registration.json5
   ```

2. **Create the filtered registry.** Acquire a new compute node. It can be any type of node and you
   only need it for an hour. Make a local copy of `scripts/create_simple_standard_scenarios.sh`,
   edit the environment variables at the top of the script to match your registry and repository
   paths, and run it. It creates a new registry database containing the StandardScenarios data
   filtered according to
   `dsgrid-test-data/filtered_registries/simple_standard_scenarios.json5`.

3. **Copy the output to the test data repository.** Copy (e.g., `cp`, `scp`, `rsync`) the output
   files (registry `.db` file and Parquet data) from the previous step to
   `dsgrid-test-data/filtered_registries/simple_standard_scenarios`. Then set the registry's
   `data_path` to the new location:

   ```bash
   sqlite3 dsgrid-test-data/filtered_registries/simple_standard_scenarios/registry.db
   ```

   ```sql
   UPDATE key_value
   SET value = 'dsgrid-test-data/filtered_registries/simple_standard_scenarios'
   WHERE key = 'data_path';
   ```

4. **Shorten file names.** Run this script from the base directory of the test data repository.
   It shortens the names of the Parquet directories and files created by Spark so that they do not
   exceed the 260-character path limit on Windows filesystems.

   ```bash
   python <path-to-dsgrid-repo>/scripts/shorten_simple_std_scns_filenames.py
   ```

5. **Open a test data pull request.** Make a branch in the test data repository, commit, push it
   to GitHub, and open a pull request.

6. **Regenerate the expected dataset statistics.** From a dsgrid repository where the
   `dsgrid-test-data` submodule contains the branch created in the previous step, run:

   ```bash
   python tests/simple_standard_scenarios_datasets.py
   ```

   This independently computes the expected ComStock, ResStock, and TEMPO project data from the
   raw dataset files and writes summary statistics to
   `dsgrid-test-data/filtered_registries/simple_standard_scenarios/expected_datasets/raw_stats.json`.
   Add the modified file to the same pull request.

Refer to [Testing](../testing.md#developing-a-feature-that-changes-code-and-data) for how to
coordinate the dsgrid and test data pull requests.
