from typing import List, Dict, Union
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *


def dedupsByRank(
    df: DataFrame,
    partitionCols: List,
    orderCols: Union[List, Dict[str, List]],
    rankType:Column = row_number(),
) -> DataFrame:
    orderCols = (
        {"columns": list(orderCols), "asc": [], "desc": []}
        if isinstance(orderCols, list)
        else orderCols
    )
    _ = (
        lambda col_name: col(col_name).asc()
        if col_name in orderCols["asc"]
        else (col(col_name).desc() if col_name in orderCols["desc"] else col(col_name))
    )
    windowBy = Window.partitionBy(
        *list(map(lambda col_name: col(col_name), partitionCols))
    ).orderBy(*list(map(lambda col_name: _(col_name), orderCols['columns'])))
    return (
        df.withColumn("rank", rankType.over(windowBy))
        .filter(col("rank") == 1)
        .drop("rank")
    )


