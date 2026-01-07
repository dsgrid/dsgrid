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

| Constant | Value |
|----------|-------|
| `ONE_WEEK_PER_MONTH_BY_HOUR` | `'one_week_per_month_by_hour'` |
| `ONE_WEEKDAY_DAY_AND_ONE_WEEKEND_DAY_PER_MONTH_BY_HOUR` | `'one_weekday_day_and_one_weekend_day_per_month_by_hour'` |

## LeapDayAdjustmentType

*dsgrid.dimension.time.LeapDayAdjustmentType*

Leap day adjustment enum types

| Constant | Value |
|----------|-------|
| `DROP_DEC31` | `'drop_dec31'` |
| `DROP_FEB29` | `'drop_feb29'` |
| `DROP_JAN1` | `'drop_jan1'` |
| `NONE` | `'none'` |

## TimeIntervalType

*dsgrid.dimension.time.TimeIntervalType*

Time interval enum types

| Constant | Value |
|----------|-------|
| `PERIOD_ENDING` | `'period_ending'` |
| `PERIOD_BEGINNING` | `'period_beginning'` |
| `INSTANTANEOUS` | `'instantaneous'` |

## MeasurementType

*dsgrid.dimension.time.MeasurementType*

Time value measurement enum types

| Constant | Value |
|----------|-------|
| `MEAN` | `'mean'` |
| `MIN` | `'min'` |
| `MAX` | `'max'` |
| `MEASURED` | `'measured'` |
| `TOTAL` | `'total'` |

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

| Constant | Value |
|----------|-------|
| `LOW` | `'low'` |
| `MODERATE` | `'moderate'` |

## DatasetQualifierType

*dsgrid.config.dataset_config.DatasetQualifierType*

str(object='') -> str
str(bytes_or_buffer[, encoding[, errors]]) -> str

Create a new string object from the given object. If encoding or
errors is specified, then the object must expose a data buffer
that will be decoded using the given encoding and error handler.
Otherwise, returns the result of object.__str__() (if defined)
or repr(object).
encoding defaults to 'utf-8'.
errors defaults to 'strict'.

| Constant | Value |
|----------|-------|
| `QUANTITY` | `'quantity'` |
| `GROWTH_RATE` | `'growth_rate'` |

## GrowthRateType

*dsgrid.config.dataset_config.GrowthRateType*

str(object='') -> str
str(bytes_or_buffer[, encoding[, errors]]) -> str

Create a new string object from the given object. If encoding or
errors is specified, then the object must expose a data buffer
that will be decoded using the given encoding and error handler.
Otherwise, returns the result of object.__str__() (if defined)
or repr(object).
encoding defaults to 'utf-8'.
errors defaults to 'strict'.

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
