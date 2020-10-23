from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from typing import List
from collections import defaultdict
from functools import reduce
from itertools import cycle


def singleJoin(
    left: DataFrame, right: DataFrame, condition=None, joinType: str = None
) -> DataFrame:
    """
    Performs a join between left and right dataframes
    Args:
        left            ->  pyspark DataFrame
        right           ->  pyspark DataFrame
        condition       ->  condition to join dataframes
        joinType        ->  type of join, default will be inner
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
         dfs            ->  list of dataframes to be joined,
                            hint can be given to each dataframe
                            List(df1,df2,df3,df4.hint('broadcast'))
         conditionList  ->  list of conditions to join the dataframes
                            List(df1.id==df2.id,df3.id==df4.id)
                            can be empty
         joinsList      ->  list of types of joins between dataframes
    Return:
         DataFrame
    Example :

    >>>first  = spark.createDataFrame([{'first_id': 1, 'value': None}, {'first_id': 2, 'value': 2}])
    >>>second = spark.createDataFrame([{'second_id': 1, 'value': 1}, {'second_id': 2, 'value': 22}])
    >>>third  = spark.createDataFrame([{'third_id': 1, 'value': 10}, {'third_id': 2, 'value': 226}])

    >>>performJoin([first,second.hint("broadcast"),third],[first.first_id==second.second_id,
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
