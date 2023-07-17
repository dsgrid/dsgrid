****************
CLI Fundamentals
****************
The dsgrid CLI tools contain some nuances that users should understand in order to have a good
experience.

General Usage
=============
The dsgrid CLI commands are hierarchical with help at every level. For example:

.. code-block:: console

   $ dsgrid
   $ dsgrid --help

   $ dsgrid registry
   $ dsgrid registry --help

   $ dsgrid query
   $ dsgrid query --help
   $ dsgrid query project run --help

Shell Completion
================
The dsgrid CLI uses the Python package `Click <https://click.palletsprojects.com>`_ to
process CLI options and arguments. Click supports shell completion for commands and subcommands for
Bash, Zsh, and Fish. We highly recommend that you configure your shell for this.

To demonstrate the value let's suppose that you want to see the commands available. Type ``dsgrid``,
a space, and then ``tab``. This is the result:

.. code-block:: console

    $ dsgrid
    config             -- Config commands
    download           -- Download a dataset.
    install-notebooks  -- Install dsgrid notebooks to a local path.
    query              -- Query group commands
    registry           -- Manage a registry.

Press ``tab`` to cycle through the options. The same principle works for subcommands (e.g., ``dsgrid
registry <tab>``).

After running the steps below restart your shell in order for the changes to take effect.

Bash Instructions
-----------------

.. code-block:: console

    $ _dsgrid_COMPLETE=bash_source dsgrid > ~/.dsgrid-complete.bash

Add this line to your ``~/.bashrc`` file::

   . ~/.dsgrid-complete.bash

Zsh Instructions
----------------

.. code-block:: console

    $ _dsgrid_COMPLETE=zsh_source dsgrid > ~/.dsgrid-complete.zsh

Add this line to your ``~/.zshrc`` file::

   . ~/.dsgrid-complete.zsh

Fish Instructions
-----------------

.. code-block:: console

   $ _dsgrid_COMPLETE=fish_source dsgrid > ~/.config/fish/completions/dsgrid.fish

Database Connection
===================

All dsgrid commands require connecting to the database. We recommend that you use
a dsgrid-provided shortcut to avoid having to type it in every command.

dsgrid RC file
--------------
dsgrid allows you to store common configuration settings in a config file in your home directory.
Here's how to create it with a database on the local computer. Change the hostname and database
name as needed.

.. code-block:: console

   $ dsgrid config create -u http://dsgrid-registry.hpc.nrel.gov:8529 -n standard-scenarios --offline
   Wrote dsgrid config to /Users/dthom/.dsgrid.json5

Environment variables
---------------------
You can also set these environment variables:

.. code-block:: console

   $ export DSGRID_REGISTRY_DATABASE_URL=http://dsgrid-registry.hpc.nrel.gov:8529

.. code-block:: console

   $ export DSGRID_REGISTRY_DATABASE_NAME=standard-scenarios
