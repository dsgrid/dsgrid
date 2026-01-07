# Enums

Enumeration types used in dsgrid configuration models.

## DimensionType

*dsgrid.dimension.base_models.DimensionType*

Dimension types

| Constant | Value |
|----------|-------|
| `METRIC` | `'metric'` |
| `GEOGRAPHY` | `'geography'` |
| `SECTOR` | `'sector'` |
| `SUBSECTOR` | `'subsector'` |
| `TIME` | `'time'` |
| `WEATHER_YEAR` | `'weather_year'` |
| `MODEL_YEAR` | `'model_year'` |
| `SCENARIO` | `'scenario'` |

## TimeDimensionType

*dsgrid.dimension.time.TimeDimensionType*

Defines the supported time formats in the load data.

| Constant | Value |
|----------|-------|
| `DATETIME` | `'datetime'` |
| `ANNUAL` | `'annual'` |
| `REPRESENTATIVE_PERIOD` | `'representative_period'` |
| `DATETIME_EXTERNAL_TZ` | `'datetime_external_tz'` |
| `INDEX` | `'index'` |
| `NOOP` | `'noop'` |

## RepresentativePeriodFormat

*dsgrid.dimension.time.RepresentativePeriodFormat*

Defines the supported formats for representative period data.

| Constant | Value | Description | frequency |
|----------|-------|-------------|-----------|
| `ONE_WEEK_PER_MONTH_BY_HOUR` | `'one_week_per_month_by_hour'` | load_data columns use 'month', 'day_of_week', 'hour' to specify time | 1:00:00 |
| `ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR` | `'one_weekday_day_and_one_weekend_day_per_month_by_hour'` | load_data columns use 'month', 'hour', 'is_weekday' to specify time | 1:00:00 |

## LeapDayAdjustmentType

*dsgrid.dimension.time.LeapDayAdjustmentType*

Leap day adjustment enum types

| Constant | Value | Description |
|----------|-------|-------------|
| `DROP_DEC31` | `'drop_dec31'` | To adjust for leap years, December 31st timestamps and data get dropped. |
| `DROP_FEB29` | `'drop_feb29'` | Feburary 29th timestamps and data are dropped. Currently not yet supported by dsgrid. |
| `DROP_JAN1` | `'drop_jan1'` | To adjust for leap years, January 1st timestamps and data get dropped. |
| `NONE` | `'none'` | No leap day adjustment made. |

## TimeIntervalType

*dsgrid.dimension.time.TimeIntervalType*

Time interval enum types

| Constant | Value | Description |
|----------|-------|-------------|
| `PERIOD_ENDING` | `'period_ending'` | A time interval that is period ending is coded by the end time. E.g., 2pm (with freq=1h) represents a period of time between 1-2pm. |
| `PERIOD_BEGINNING` | `'period_beginning'` | A time interval that is period beginning is coded by the beginning time. E.g., 2pm (with freq=01:00:00) represents a period of time between 2-3pm. This is the dsgrid default. |
| `INSTANTANEOUS` | `'instantaneous'` | The time record value represents measured, instantaneous time |

## MeasurementType

*dsgrid.dimension.time.MeasurementType*

Time value measurement enum types

| Constant | Value | Description |
|----------|-------|-------------|
| `MEAN` | `'mean'` | Data values represent the average value in a time range |
| `MIN` | `'min'` | Data values represent the minimum value in a time range |
| `MAX` | `'max'` | Data values represent the maximum value in a time range |
| `MEASURED` | `'measured'` | Data values represent the measured value at that reported time |
| `TOTAL` | `'total'` | Data values represent the sum of values in a time range |

## DatasetRegistryStatus

*dsgrid.registry.common.DatasetRegistryStatus*

Statuses for a dataset within a project

| Constant | Value |
|----------|-------|
| `UNREGISTERED` | `'Unregistered'` |
| `REGISTERED` | `'Registered'` |

## ProjectRegistryStatus

*dsgrid.registry.common.ProjectRegistryStatus*

Statuses for a project within the DSGRID registry

| Constant | Value |
|----------|-------|
| `INITIAL_REGISTRATION` | `'Initial Registration'` |
| `IN_PROGRESS` | `'In Progress'` |
| `COMPLETE` | `'Complete'` |
| `PUBLISHED` | `'Published'` |
| `DEPRECATED` | `'Deprecated'` |

## InputDatasetType

*dsgrid.config.dataset_config.InputDatasetType*

dsgrid Enum class

| Constant | Value |
|----------|-------|
| `MODELED` | `'modeled'` |
| `HISTORICAL` | `'historical'` |
| `BENCHMARK` | `'benchmark'` |
| `UNSPECIFIED` | `'unspecified'` |

## DataClassificationType

*dsgrid.config.dataset_config.DataClassificationType*

Data risk classification type.

See FIPS 199, https://csrc.nist.gov/files/pubs/fips/199/final/docs/fips-pub-199-final.pdf
for more information. In general these classifications describe potential impact on
organizations and individuals. In more detailed schemes a separate classification could
be applied to confidentiality, integrity, and availability.

| Constant | Value | Description |
|----------|-------|-------------|
| `LOW` | `'low'` | The loss of confidentiality, integrity, or availability could be expected to have a limited adverse effect on organizational operations, organizational assets, or individuals. |
| `MODERATE` | `'moderate'` | The loss of confidentiality, integrity, or availability could be expected to have a serious adverse effect on organizational operations, organizational assets, or individuals. |

## DatasetQualifierType

*dsgrid.config.dataset_config.DatasetQualifierType*

| Constant | Value |
|----------|-------|
| `QUANTITY` | `'quantity'` |
| `GROWTH_RATE` | `'growth_rate'` |

## GrowthRateType

*dsgrid.config.dataset_config.GrowthRateType*

| Constant | Value |
|----------|-------|
| `EXPONENTIAL_ANNUAL` | `'exponential_annual'` |
| `EXPONENTIAL_MONTHLY` | `'exponential_monthly'` |

## DimensionMappingType

*dsgrid.config.dimension_mapping_base.DimensionMappingType*

Defines the operation dsgrid will apply to the data during a mapping.

| Constant | Value |
|----------|-------|
| `ONE_TO_ONE` | `'one_to_one'` |
| `MANY_TO_ONE_AGGREGATION` | `'many_to_one_aggregation'` |
| `MANY_TO_ONE_REASSIGNMENT` | `'many_to_one_reassignment'` |
| `DUPLICATION` | `'duplication'` |
| `ONE_TO_MANY_DISAGGREGATION` | `'one_to_many_disaggregation'` |
| `MANY_TO_MANY_AGGREGATION` | `'many_to_many_aggregation'` |
| `MANY_TO_MANY_DISAGGREGATION` | `'many_to_many_disaggregation'` |
| `MANY_TO_ONE_ASSIGNMENT` | `'many_to_one_assignment'` |
| `ONE_TO_MANY_ASSIGNMENT` | `'one_to_many_assignment'` |
| `MANY_TO_MANY_ASSIGNMENT` | `'many_to_many_assignment'` |
| `ONE_TO_ONE_EXPLICIT_MULTIPLIERS` | `'one_to_one_explicit_multipliers'` |
| `ONE_TO_MANY_EXPLICIT_MULTIPLIERS` | `'one_to_many_explicit_multipliers'` |
| `MANY_TO_ONE_EXPLICIT_MULTIPLIERS` | `'many_to_one_explicit_multipliers'` |
| `MANY_TO_MANY_EXPLICIT_MULTIPLIERS` | `'many_to_many_explicit_multipliers'` |

## DimensionMappingArchetype

*dsgrid.config.dimension_mapping_base.DimensionMappingArchetype*

Dimension mapping archetype, used to check whether duplicates are allowed in from/to
dimensions and apply rules about the sum of the from_fraction column.

| Constant | Value | Description | allow_dup_from_records | allow_dup_to_records | check_fraction_sum_eq1_from_id | check_fraction_sum_eq1_to_id |
|----------|-------|-------------|------------------------|----------------------|--------------------------------|------------------------------|
| `ONE_TO_ONE_MAP_FRACTION_SUM_FROM_ID_EQ1` | `'one_to_one_map_fraction_sum_from_id_eq1'` | One-to-one dimension mapping with sum of from_fraction = 1 when grouped by from_id | False | False | True | False |
| `ONE_TO_MANY_MAP_FRACTION_SUM_FROM_ID_EQ1` | `'one_to_many_map_fraction_sum_from_id_eq1'` | One-to-many dimension mapping with sum of from_fraction = 1 when grouped by from_id | True | False | True | False |
| `MANY_TO_ONE_MAP_FRACTION_SUM_FROM_ID_EQ1` | `'many_to_one_map_fraction_sum_from_id_eq1'` | Many-to-one dimension mapping with sum of from_fraction = 1 when grouped by from_id | False | True | True | False |
| `MANY_TO_MANY_MAP_FRACTION_SUM_FROM_ID_EQ1` | `'many_to_many_map_fraction_sum_from_id_eq1'` | Many-to-many dimension mapping with sum of from_fraction = 1 when grouped by from_id | True | True | True | False |
| `ONE_TO_ONE_MAP_FRACTION_SUM_TO_ID_EQ1` | `'one_to_one_map_fraction_sum_to_id_eq1'` | One-to-one dimension mapping with sum of from_fraction = 1 when grouped by to_id | False | False | False | True |
| `ONE_TO_MANY_MAP_FRACTION_SUM_TO_ID_EQ1` | `'one_to_many_map_fraction_sum_to_id_eq1'` | One-to-many dimension mapping with sum of from_fraction = 1 when grouped by to_id | True | False | False | True |
| `MANY_TO_ONE_MAP_FRACTION_SUM_TO_ID_EQ1` | `'many_to_one_map_fraction_sum_to_id_eq1'` | Many-to-one dimension mapping with sum of from_fraction = 1 when grouped by to_id | False | True | False | True |
| `MANY_TO_MANY_MAP_FRACTION_SUM_TO_ID_EQ1` | `'many_to_many_map_fraction_sum_to_id_eq1'` | Many-to-many dimension mapping with sum of from_fraction = 1 when grouped by to_id | True | True | False | True |
| `ONE_TO_ONE_MAP` | `'one_to_one_map'` | One-to-one dimension mapping with no from_fraction sum check | False | False | False | False |
| `ONE_TO_MANY_MAP` | `'one_to_many_map'` | One-to-many dimension mapping with no from_fraction sum check | True | False | False | False |
| `MANY_TO_ONE_MAP` | `'many_to_one_map'` | Many-to-one dimension mapping with no from_fraction sum check | False | True | False | False |
| `MANY_TO_MANY_MAP` | `'many_to_many_map'` | Many-to-many dimension mapping with no from_fraction sum check | True | True | False | False |
