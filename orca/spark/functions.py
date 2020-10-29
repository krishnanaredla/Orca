from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from typing import List, Dict, Callable, Any, Type
from collections import defaultdict
from functools import reduce
from itertools import cycle
import re
import datetime
import decimal


def singleJoin(
    left: DataFrame, right: DataFrame, condition=None, joinType: str = None
) -> DataFrame:
    """
    Performs a join between left and right dataframes
    Args:
        left            :  pyspark DataFrame
        right           :  pyspark DataFrame
        condition       :  condition to join dataframes
        joinType        :  type of join, default will be inner
    Return:
        DataFrame
    """
    return left.join(right, condition, joinType)


def performJoin(
    dfs: List[DataFrame], conditionsList: List = [], joinsList: List[str] = ["inner"]
) -> DataFrame:
    """
    Joins multiple dataframes with varying conditions and join types for each join
    Args:
         dfs            :  list of dataframes to be joined,
                            hint can be given to each dataframe
                            List(df1,df2,df3,df4.hint('broadcast'))
         conditionList  :  list of conditions to join the dataframes
                            List(df1.id==df2.id,df3.id==df4.id)
                            can be empty
         joinsList      :  list of types of joins between dataframes
    Return:
         DataFrame
    Usage :

    >>> first  = spark.createDataFrame([{'first_id': 1, 'value': None}, {'first_id': 2, 'value': 2}])
    >>> second = spark.createDataFrame([{'second_id': 1, 'value': 1}, {'second_id': 2, 'value': 22}])
    >>> third  = spark.createDataFrame([{'third_id': 1, 'value': 10}, {'third_id': 2, 'value': 226}])

    >>> performJoin([first,second.hint("broadcast"),third],[first.first_id==second.second_id,
                                 second.second_id==third.third_id]).show()
        +--------+-----+---------+-----+--------+-----+
        |first_id|value|second_id|value|third_id|value|
        +--------+-----+---------+-----+--------+-----+
        |       1| null|        1|    1|       1|   10|
        |       2|    2|        2|   22|       2|  226|
        +--------+-----+---------+-----+--------+-----+

    """
    frames = iter(dfs)
    conditions = cycle(conditionsList)
    joins = cycle(joinsList)
    value = next(frames)
    for element in frames:
        try:
            nextCondition = next(conditions)
        except StopIteration as e:
            nextCondition = None
        # nextJoin      = lambda x : None if next(joins) is None else next(joins)
        value = singleJoin(value, element, nextCondition, next(joins))
    return value


def multijoin(
    dfs: List[DataFrame],
    on: str = None,
    how: str = None,
    coalesce: List[str] = None,
):
    """Join multiple dataframes.
    Args:
        dfs (list[DataFrame]).
        on: same as ``pyspark.sql.DataFrame.join``.
        how: same as ``pyspark.sql.DataFrame.join``.
        coalesce (list[str]): column names to disambiguate by coalescing
            across the input dataframes. A column must be of the same type
            across all dataframes that define it; if different types appear
            coalesce will do a best-effort attempt in merging them. The
            selected value is the first non-null one in order of appearance
            of the dataframes in the input list. Default is None - don't
            coalesce any ambiguous columns.
    Returns:
        pyspark.sql.DataFrame or None if provided dataframe list is empty.
    Example:
        Assume we have two DataFrames, the first is
        ``first = [{'id': 1, 'value': None}, {'id': 2, 'value': 2}]``
        and the second is
        ``second = [{'id': 1, 'value': 1}, {'id': 2, 'value': 22}]``
        Then collecting the ``DataFrame`` produced by
        ``multijoin([first, second], on='id', how='inner', coalesce=['value'])``
        yields ``[{'id': 1, 'value': 1}, {'id': 2, 'value': 2}]``.
    """
    if not dfs:
        return None

    # Go over the input dataframes and rename each to-be-resolved
    # column to ensure name uniqueness
    coalesce = set(coalesce or [])
    renamed_columns = defaultdict(list)
    for idx, df in enumerate(dfs):
        for col in df.columns:
            if col in coalesce:
                disambiguation = "__{}_{}".format(idx, col)
                df = df.withColumnRenamed(col, disambiguation)
                renamed_columns[col].append(disambiguation)
        dfs[idx] = df

    # Join the dataframes
    joined_df = reduce(lambda x, y: x.join(y, on=on, how=how), dfs)

    # And coalesce the would-have-been-ambiguities
    for col, disambiguations in renamed_columns.items():
        joined_df = joined_df.withColumn(col, F.coalesce(*disambiguations))
        for disambiguation in disambiguations:
            joined_df = joined_df.drop(disambiguation)

    return joined_df


def addColumnPrefix(
    df: DataFrame, prefix: str = "prefix", colsList: List = []
) -> DataFrame:
    """
    Adds a prefix to Column names
    Args:
        df       : pyspark DataFrame
        prefix   : prefix string which needs to be added default value
                    will be `prefix`
        colsList : Specify the column names to which only the prefix will be added.
                    If nothing is passed, all columns will have the prefix.
                    Default value will be empty list
    Return:
        pyspark Dataframe
    Usage :
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

    """
    if not colsList:
        colsList = df.columns

    def _keyExists(s):
        return s in colsList

    cols = list(
        map(
            lambda col_name: F.col(col_name).alias("{0}_{1}".format(prefix, col_name))
            if _keyExists(col_name)
            else F.col(col_name),
            df.columns,
        )
    )
    return df.select(*cols)


def addColumnSuffix(
    df: DataFrame, suffix: str = "suffix", colsList: List = []
) -> DataFrame:
    """
    Adds a suffix to Column names
    Args:
        df       : pyspark DataFrame
        suffix   : suffix string which needs to be added default value
                    will be `suffix`
        colsList : Specify the column names to which only the suffix will be added.
                    If nothing is passed, all columns will have the suffix.
                    Default value will be empty list
    Return:
        pyspark Dataframe
    Usage :
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
    """
    if not colsList:
        colsList = df.columns

    def _keyExists(s):
        return s in colsList

    cols = list(
        map(
            lambda col_name: F.col(col_name).alias("{0}_{1}".format(col_name, suffix))
            if _keyExists(col_name)
            else F.col(col_name),
            df.columns,
        )
    )
    return df.select(*cols)


def removeColumnSpaces(df: DataFrame) -> DataFrame:
    """
    Adds a suffix to Column names
    Args:
        df       : pyspark DataFrame
    Return:
        pyspark Dataframe
    Usage :

    >>> df = spark.createDataFrame([{'id': 1, 'goods value': 1,'total amount':2}])
    >>> removeColumnSpaces(df).show()
        +----------+---+-----------+
        |goodsvalue| id|totalamount|
        +----------+---+-----------+
        |         1|  1|          2|
        +----------+---+-----------+

    """
    return df.select(
        [
            F.col(c).alias("{0}".format(re.sub(r"\s+", "", c, flags=re.UNICODE)))
            for c in df.columns
        ]
    )


def withSomeColumnsRenamed(df: DataFrame, mapping: Dict) -> DataFrame:
    """
    Changes some of the column names with supplied dictionary
    Args:
        df       : pyspark DataFrame
        mapping  : Dict
    Return:
        pyspark Dataframe

    Usage :

    >>> df = spark.createDataFrame([{'id': 1, 'value': 1,'amount':2}])
    >>> mapping = {'amount':'cash','id':'uniqueID','value':'transaction'}
    >>> withSomeColumnsRenamed(df,mapping).show()
        +----+--------+-----------+
        |cash|uniqueID|transaction|
        +----+--------+-----------+
        |   2|       1|          1|
        +----+--------+-----------+


    """

    def _keys(s):
        return mapping[s]

    def _keysExists(s):
        return s in mapping

    cols = list(
        map(
            lambda col_name: F.col(col_name).alias(_keys(col_name))
            if _keysExists(col_name)
            else F.col(col_name),
            df.columns,
        )
    )
    return df.select(*cols)


def withColumnsRenamedFunc(df: DataFrame, func: Callable) -> DataFrame:
    """
    Changes some of the column names with supplied function
    Args:
        df       : pyspark DataFrame
        func     : Function
    Return:
        pyspark Dataframe
    Usage :
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


    """
    cols = list(map(lambda col_name: F.col(col_name).alias(func(col_name)), df.columns))
    return df.select(*cols)


# Mapping Python types to Spark SQL DataType
_type_mappings = {
    type(None): NullType,
    bool: BooleanType,
    int: LongType,
    float: DoubleType,
    str: StringType,
    bytearray: BinaryType,
    decimal.Decimal: DecimalType,
    datetime.date: DateType,
    datetime.datetime: TimestampType,
    datetime.time: TimestampType,
    bytes: BinaryType,
}


def selectByDtype(df: DataFrame, dtypes: Any) -> DataFrame:
    if isinstance(dtypes, list):
        for val in range(len(dtypes)):
            if dtypes[val] in _type_mappings.keys():
                dtypes[val] = _type_mappings[dtypes[val]]
        dtypes = tuple(dtypes)
    elif dtypes in _type_mappings.keys():
        dtypes = _type_mappings[dtypes]
    cols = list(
        filter(
            None.__ne__,
            list(
                map(
                    lambda field: field.name
                    if isinstance(field.dataType, dtypes)
                    else None,
                    df.schema,
                )
            ),
        )
    )
    return df.select(*cols)


def removeByDtype(df: DataFrame, dtypes: Any) -> DataFrame:
    if isinstance(dtypes, list):
        for val in range(len(dtypes)):
            if dtypes[val] in _type_mappings.keys():
                dtypes[val] = _type_mappings[dtypes[val]]
        dtypes = tuple(dtypes)
    elif dtypes in _type_mappings.keys():
        dtypes = _type_mappings[dtypes]
    cols = list(
        filter(
            None.__ne__,
            list(
                map(
                    lambda field: field.name
                    if isinstance(field.dataType, dtypes)
                    else None,
                    df.schema,
                )
            ),
        )
    )
    return df.select(*[col for col in df.columns if col not in cols])


def getColsByDtype(df: DataFrame, dtypes: Any) -> List:
    if isinstance(dtypes, list):
        for val in range(len(dtypes)):
            if dtypes[val] in _type_mappings.keys():
                dtypes[val] = _type_mappings[dtypes[val]]
        dtypes = tuple(dtypes)
    elif dtypes in _type_mappings.keys():
        dtypes = _type_mappings[dtypes]
    cols = list(
        filter(
            None.__ne__,
            list(
                map(
                    lambda field: field.name
                    if isinstance(field.dataType, dtypes)
                    else None,
                    df.schema,
                )
            ),
        )
    )
    return cols
