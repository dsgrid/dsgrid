# Testing

dsgrid uses [pytest](https://docs.pytest.org). Complete [Development Setup](development_setup.md)
before running the tests.

## Test Data Submodule

The tests use the [dsgrid-test-data](https://github.com/dsgrid/dsgrid-test-data) repository as a
git submodule in `./dsgrid-test-data`. It contains small registries and datasets, including a
filtered version of the StandardScenarios project. You must initialize this submodule and keep it
updated.

Initialize the submodule:

```bash
git submodule update --init --recursive
```

Update the submodule when there are new changes in the test data repository:

```bash
git submodule update --remote --merge
```

## Run the Tests

Run the test suite with the default DuckDB backend:

```bash
pytest tests dsgrid/utils/py_expression_eval/tests.py
```

`pytest` with no arguments runs only `tests`, as configured in `pyproject.toml`. Refer to
`.github/workflows/pull_request_tests.yml` to see exactly how CI runs the tests.

### Spark Backend

To run the tests with the Spark backend, install the `spark` extra, make sure
[Java 17 or 21](../getting_started/installation.md#install-java) is available, and set the backend
with an environment variable:

```bash
DSGRID_BACKEND_ENGINE=spark pytest tests
```

No separate Spark installation is needed; the tests run Spark in local mode through `pyspark`.

On an HPC compute node, point Spark at a scratch directory with enough space:

```bash
export SPARK_LOCAL_DIRS=/tmp/scratch/<slurm-job-id>
```

If you instead started a Spark cluster with a custom configuration, set
`export SPARK_CONF_DIR=<path-to-your-conf-dir>` instead.

### Useful pytest Options

| Option | Effect |
| --- | --- |
| `--log-cli-level=debug` | Emits log messages to the console. The level can be `debug`, `info`, `warning`, or `error`. |
| `-k <expression>` | Runs only tests whose names match the expression. |
| `-x` | Stops after the first failure. |

## Test Registries

The tests create their own registries and clean up after themselves.

To save time, the test setup creates a shared cached registry at
`tests/data/registry/cached_registry.db` and records the current git commit in
`tests/data/registry/commit.txt`. The cached registry is only rebuilt after a new commit. Delete
`tests/data/registry` to force a rebuild.

If you change the configs or data for the StandardScenarios project, you will also need to
[update the simple-standard-scenarios test registry](how_tos/update_test_registry.md).

## Developing a Feature That Changes Code and Data

If you are developing a feature that requires changing both code and test data, you need to keep
the submodule synchronized. Here is an example workflow:

1. Create a dsgrid branch.

   ```bash
   git checkout -b feature-x
   ```

2. Create a data branch inside the submodule.

   ```bash
   cd dsgrid-test-data
   git checkout -b feature-x
   cd ..
   ```

3. Implement and test your feature.

4. Commit your code and data changes in each repository.

5. Update the dsgrid repository to point to the correct data branch and commit.

   ```bash
   git submodule set-branch -b feature-x dsgrid-test-data
   git add .gitmodules dsgrid-test-data
   git commit -m "Point dsgrid-test-data to feature-x branch"
   ```

6. Push both branches to GitHub. **Note**: Using a forked data repository is not supported.

7. Open two pull requests.

8. Address comments, if needed. If you make new commits to the data branch, then you must update
   the dsgrid branch before pushing back to GitHub.

   ```bash
   git add dsgrid-test-data
   git commit -m "Update dsgrid-test-data"
   ```

9. Merge the data pull request.

10. Update the dsgrid branch to point back to the `main` data branch.

    ```bash
    cd dsgrid-test-data
    git checkout main
    git pull origin main
    # The feature branch is now in main. Delete it.
    git branch -d feature-x
    cd ..
    git submodule set-branch -b main dsgrid-test-data
    git add .gitmodules dsgrid-test-data
    git commit -m "Point dsgrid-test-data to main branch"
    git push origin feature-x
    ```

11. After CI passes, merge the dsgrid pull request.
