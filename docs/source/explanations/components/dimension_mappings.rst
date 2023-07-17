******************
Dimension Mappings
******************
A dimension mapping is defined by its :ref:`dimension-mapping-config`. This data structure
describes the mapping's attributes and records.

A mapping can simply map one value to another or perform aggregations and disaggregations. It can optionally apply multipliers. The
behavior is dictated by the ``mapping_type`` field. Please refer to :ref:`dimension-mapping-type`
for options (click on the ``source`` link).

Examples
========
The `dsgrid-StandardScenarios repository
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/tree/main/dsgrid_project>`_
contains datasets that you can use as examples.

Many-To-One Aggregration
------------------------
The `project config
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/project.json5>`_
defines a mapping of counties to state that can be used to aggregate energy use values. This is a
base-to-supplemental dimension mapping. The file defines the mapping like this:

.. code-block:: JavaScript

      {
        description: 'Maps US Counties 2020 L48 to State',
        file: 'dimension_mappings/base_to_supplemental/lookup_county_to_state.csv',
        mapping_type: 'many_to_one_aggregation',
        from_dimension: {
          name: 'US Counties 2020 L48',
          type: 'geography',
        },
        to_dimension: {
          name: 'US States L48',
          type: 'geography',
        },
      }

The first few lines of the records file looks like this::

    from_id,to_id
    01001,AL
    01003,AL
    01005,AL
    01007,AL
    01009,AL

Many-To-Many Explicit Multipliers
---------------------------------
The `TEMPO dataset config
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/tempo/dimension_mappings.json5>`_
defines an interpolation-based mapping for model years because the dataset only contain even years.
When mapping to the project dsgrid will use these records to interpolate the values for odd years.

The mapping config looks like this:

.. code-block:: JavaScript

    {
      description: "2010-2050 from interpolating for every other year and 0 for 2010-2017",
      dimension_type: "model_year",
      file: "dimension_mappings/model_year_to_model_year.csv",
      mapping_type: "many_to_many_explicit_multipliers",
    }

A sample of the records file looks like this::

    from_id,to_id,from_fraction
    2018,2017,0
    2018,2018,1
    2018,2019,0.5
    2020,2019,0.5
    2020,2020,1
