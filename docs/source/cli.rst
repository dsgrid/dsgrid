CLI
###########

.. command-output:: dsgrid --help



query cli
************
`dsgrid query` cli is current under development.:red:`Coming Soon. This is currently under development.`

download cli
************
`dsgrid download` cli provides an interface for downloading dsgrid data. 
:red:`Coming Soon. This is currently under development.`

registry cli
************

.. codeblock:: 
    ```
    Usage: dsgrid registry [OPTIONS] COMMAND [ARGS]...

    Manage a registry.

    Options:
    --path TEXT  INTERNAL-ONLY: path to dsgrid registry. Override with the
                environment variable DSGRID_REGISTRY_PATH  [default:
                /Users/mmooney/.dsgrid-registry]

    --help       Show this message and exit.

    Commands:
    create            Create a new registry.
    list              List the contents of a registry.
    register-project  Register a new project with the dsgrid repository.
    remove-dataset    Remove a dataset from the dsgrid repository.
    remove-project    Remove a project from the dsgrid repository.
    submit-dataset    Submit a new dataset to a dsgrid project.
    update-project    Update an existing project registry.
    ```

.. click:: dsgrid.cli.registry:registry
   :prog: dsgrid registry
   :nested: full