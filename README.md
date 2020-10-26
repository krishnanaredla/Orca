# Functions

> Auto-generated documentation for [orca.spark.functions](https://github.com/krishnanaredla/Orca/blob/masterorca/spark/functions.py) module.

- [Orca](..\..\README.md#orca-index) / [Modules](..\..\MODULES.md#orca-modules) / [Orca](..\index.md#orca) / [Spark](index.md#spark) / Functions
    - [multijoin](#multijoin)
    - [performJoin](#performjoin)
    - [singleJoin](#singlejoin)

## multijoin

[[find in source code]](https://github.com/krishnanaredla/Orca/blob/master/orca/spark/functions.py#L72)

```python
def multijoin(
    dfs: List[DataFrame],
    on: str = None,
    how: str = None,
    coalesce: List[str] = None,
):
```

Join multiple dataframes.

#### Arguments

-  dfs (list[DataFrame]).
- `on` - same as ``pyspark.sql.DataFrame.join``.
- `how` - same as ``pyspark.sql.DataFrame.join``.
- `coalesce` *list[str]* - column names to disambiguate by coalescing
    across the input dataframes. A column must be of the same type
    across all dataframes that define it; if different types appear
    coalesce will do a best-effort attempt in merging them. The
    selected value is the first non-null one in order of appearance
    of the dataframes in the input list. Default is None - don't
    coalesce any ambiguous columns.

#### Returns

pyspark.sql.DataFrame or None if provided dataframe list is empty.

#### Examples

Assume we have two DataFrames, the first is
``first = [{'id': 1, 'value': None}, {'id': 2, 'value': 2}]``
and the second is
``second = [{'id': 1, 'value': 1}, {'id': 2, 'value': 22}]``
Then collecting the ``DataFrame`` produced by
``multijoin([first, second], on='id', how='inner', coalesce=['value'])``
- `yields` *``[{'id'* - 1, 'value': 1}, {'id': 2, 'value': 2}]``.

## performJoin

[[find in source code]](https://github.com/krishnanaredla/Orca/blob/masterorca/spark/functions.py#L27)

```python
def performJoin(
    dfs: List[DataFrame],
    conditionsList: List = [],
    joinsList: List[str] = ['inner'],
) -> DataFrame:
```

Joins multiple dataframes with varying conditions and join types for each join

#### Arguments

dfs            ->  list of dataframes to be joined,
                   hint can be given to each dataframe
                   List(df1,df2,df3,df4.hint('broadcast'))
conditionList  ->  list of conditions to join the dataframes
                   List(df1.id==df2.id,df3.id==df4.id)
                   can be empty
joinsList      ->  list of types of joins between dataframes

#### Returns

     DataFrame
Example :

```python
>>>first  = spark.createDataFrame([{'first_id': 1, 'value': None}, {'first_id': 2, 'value': 2}])
>>>second = spark.createDataFrame([{'second_id': 1, 'value': 1}, {'second_id': 2, 'value': 22}])
>>>third  = spark.createDataFrame([{'third_id': 1, 'value': 10}, {'third_id': 2, 'value': 226}])
```

```python
>>>performJoin([first,second.hint("broadcast"),third],[first.first_id==second.second_id,
                             second.second_id==third.third_id]).show()
    +--------+-----+---------+-----+--------+-----+
    |first_id|value|second_id|value|third_id|value|
    +--------+-----+---------+-----+--------+-----+
    |       1| null|        1|    1|       1|   10|
    |       2|    2|        2|   22|       2|  226|
    +--------+-----+---------+-----+--------+-----+
```

## singleJoin

[[find in source code]](https://github.com/krishnanaredla/Orca/blob/masterorca/spark/functions.py#L11)

```python
def singleJoin(
    left: DataFrame,
    right: DataFrame,
    condition=None,
    joinType: str = None,
) -> DataFrame:
```

Performs a join between left and right dataframes

#### Arguments

left            ->  pyspark DataFrame
right           ->  pyspark DataFrame
condition       ->  condition to join dataframes
joinType        ->  type of join, default will be inner

#### Returns

DataFrame
