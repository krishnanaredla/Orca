# Functions

> Auto-generated documentation for [orca.spark.functions](..\..\..\orca\spark\functions.py) module.

- [Orca](..\..\README.md#functions) / [Modules](..\..\MODULES.md#orca-modules) / [Orca](..\index.md#orca) / [Spark](index.md#spark) / Functions
    - [addColumnPrefix](#addcolumnprefix)
    - [addColumnSuffix](#addcolumnsuffix)
    - [multijoin](#multijoin)
    - [performJoin](#performjoin)
    - [removeColumnSpaces](#removecolumnspaces)
    - [singleJoin](#singlejoin)
    - [withColumnsRenamedFunc](#withcolumnsrenamedfunc)
    - [withSomeColumnsRenamed](#withsomecolumnsrenamed)

## addColumnPrefix

[[find in source code]](..\..\..\orca\spark\functions.py#L129)

```python
def addColumnPrefix(
    df: DataFrame,
    prefix: str = 'prefix',
    colsList: List = [],
) -> DataFrame:
```

Adds a prefix to Column names

#### Arguments

df       : pyspark DataFrame
prefix   : prefix string which needs to be added default value
            will be `prefix`
colsList : Specify the column names to which only the prefix will be added.
            If nothing is passed, all columns will have the prefix.
            Default value will be empty list

#### Returns

    pyspark Dataframe
Usage :

```python
>>> df = spark.createDataFrame([{'id': 1, 'value': 1,'amount':2}, {'id': 2, 'value': 2,'amount':3}])
>>> addColumnPrefix(df,prefix='test').show()
        +-----------+-------+----------+
        |test_amount|test_id|test_value|
        +-----------+-------+----------+
        |          2|      1|         1|
        |          3|      2|         2|
        +-----------+-------+----------+
>>> addColumnPrefix(data,prefix='test').show()
        +-----------+---+----------+
        |test_amount| id|test_value|
        +-----------+---+----------+
        |          2|  1|         1|
        |          3|  2|         2|
        +-----------+---+----------+
```
## addColumnSuffix

[[find in source code]](..\..\..\orca\spark\functions.py#L178)

```python
def addColumnSuffix(
    df: DataFrame,
    suffix: str = 'suffix',
    colsList: List = [],
) -> DataFrame:
```

Adds a suffix to Column names

#### Arguments

df       : pyspark DataFrame
suffix   : suffix string which needs to be added default value
            will be `suffix`
colsList : Specify the column names to which only the suffix will be added.
            If nothing is passed, all columns will have the suffix.
            Default value will be empty list

#### Returns

    pyspark Dataframe
Usage :

```python
>>> df = spark.createDataFrame([{'id': 1, 'value': 1,'amount':2}, {'id': 2, 'value': 2,'amount':3}])
>>> addColumnSuffix(df,suffix='test').show()
        +-----------+-------+----------+
        |amount_test|id_test|value_test|
        +-----------+-------+----------+
        |          2|      1|         1|
        |          3|      2|         2|
        +-----------+-------+----------+
>>> addColumnSuffix(df,suffix='test',colsList=['id']).show()
        +-----------+---+----------+
        |amount_test| id|value_test|
        +-----------+---+----------+
        |          2|  1|         1|
        |          3|  2|         2|
        +-----------+---+----------+
```
## multijoin

[[find in source code]](..\..\..\orca\spark\functions.py#L73)

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

dfs (list[DataFrame]).
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

[[find in source code]](..\..\..\orca\spark\functions.py#L28)

```python
def performJoin(
    dfs: List[DataFrame],
    conditionsList: List = [],
    joinsList: List[str] = ['inner'],
) -> DataFrame:
```

Joins multiple dataframes with varying conditions and join types for each join

#### Arguments

dfs            :  list of dataframes to be joined,
                   hint can be given to each dataframe
                   List(df1,df2,df3,df4.hint('broadcast'))
conditionList  :  list of conditions to join the dataframes
                   List(df1.id==df2.id,df3.id==df4.id)
                   can be empty
joinsList      :  list of types of joins between dataframes

#### Returns

     DataFrame
Usage :

```python
>>> first  = spark.createDataFrame([{'first_id': 1, 'value': None}, {'first_id': 2, 'value': 2}])
>>> second = spark.createDataFrame([{'second_id': 1, 'value': 1}, {'second_id': 2, 'value': 22}])
>>> third  = spark.createDataFrame([{'third_id': 1, 'value': 10}, {'third_id': 2, 'value': 226}])
```

```python
>>> performJoin([first,second.hint("broadcast"),third],[first.first_id==second.second_id,
                             second.second_id==third.third_id]).show()
    +--------+-----+---------+-----+--------+-----+
    |first_id|value|second_id|value|third_id|value|
    +--------+-----+---------+-----+--------+-----+
    |       1| null|        1|    1|       1|   10|
    |       2|    2|        2|   22|       2|  226|
    +--------+-----+---------+-----+--------+-----+
```
## removeColumnSpaces

[[find in source code]](..\..\..\orca\spark\functions.py#L226)

```python
def removeColumnSpaces(df: DataFrame) -> DataFrame:
```

Adds a suffix to Column names

#### Arguments

df       : pyspark DataFrame

#### Returns

    pyspark Dataframe
Usage :

```python
>>> df = spark.createDataFrame([{'id': 1, 'goods value': 1,'total amount':2}])
>>> removeColumnSpaces(df).show()
    +----------+---+-----------+
    |goodsvalue| id|totalamount|
    +----------+---+-----------+
    |         1|  1|          2|
    +----------+---+-----------+
```
## singleJoin

[[find in source code]](..\..\..\orca\spark\functions.py#L12)

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

left            :  pyspark DataFrame
right           :  pyspark DataFrame
condition       :  condition to join dataframes
joinType        :  type of join, default will be inner

#### Returns

DataFrame

## withColumnsRenamedFunc

[[find in source code]](..\..\..\orca\spark\functions.py#L292)

```python
def withColumnsRenamedFunc(df: DataFrame, func: Callable) -> DataFrame:
```

Changes some of the column names with supplied function

#### Arguments

df       : pyspark DataFrame
func     : Function

#### Returns

    pyspark Dataframe
Usage :

```python
>>> df = spark.createDataFrame([{'id': 1, 'value': 1,'amount':2}])
>>> def renameF(s):
        if 'amount' in s:
            return 'cash'
        else:
            return s
>>> withColumnsRenamedFunc(df,renameF).show()
    +----+---+-----+
    |cash| id|value|
    +----+---+-----+
    |   2|  1|    1|
    |   3|  2|    2|
    +----+---+-----+

## withSomeColumnsRenamed

[[find in source code]](..\..\..\orca\spark\functions.py#L252)

```python
def withSomeColumnsRenamed(df: DataFrame, mapping: Dict) -> DataFrame:
```

Changes some of the column names with supplied dictionary

#### Arguments

df       : pyspark DataFrame
mapping  : Dict

#### Returns

pyspark Dataframe

Usage :

```python
>>> df = spark.createDataFrame([{'id': 1, 'value': 1,'amount':2}])
>>> mapping = {'amount':'cash','id':'uniqueID','value':'transaction'}
>>> withSomeColumnsRenamed(df,mapping).show()
    +----+--------+-----------+
    |cash|uniqueID|transaction|
    +----+--------+-----------+
    |   2|       1|          1|
    +----+--------+-----------+
