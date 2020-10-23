import pytest
from orca.spark.functions import *
from chispa.dataframe_comparer import *
from tests.conftest import *
from tests.conftest import auto_inject_fixtures

@auto_inject_fixtures('spark_test_session')

def test_singleJoin(spark_test_session):
    """
    Test the single join 
    """
    first        = spark_test_session.createDataFrame([{'first_id': 1, 'value': None}, {'first_id': 2, 'value': 2}])
    second       = spark_test_session.createDataFrame([{'second_id': 1, 'value': 1}, {'second_id': 2, 'value': 22}])
    expected_df  = first.join(second,first.first_id==second.second_id,"inner")
    output_df    = singleJoin(first,second,first.first_id==second.second_id)
    assert_df_equality(output_df, expected_df)
    assert all([a == b for a, b in zip(output_df.columns, expected_df.columns)])


def test_performJoin(spark_test_session):
    """
    Test the perform Join 
    """
    first        = spark_test_session.createDataFrame([{'first_id': 1, 'value': None}, {'first_id': 2, 'value': 2}])
    second       = spark_test_session.createDataFrame([{'second_id': 1, 'value': 1}, {'second_id': 2, 'value': 22}])
    third        = spark_test_session.createDataFrame([{'third_id': 1, 'value': 10}, {'third_id': 2, 'value': 226}])
    expected_df  = first.join(second.hint("broadcast"),
                          first.first_id==second.second_id,"inner")\
                        .join(third,second.second_id==third.third_id,"inner")
    output_df    = performJoin([first,second.hint("broadcast"),third],
                               [first.first_id==second.second_id,
                                 second.second_id==third.third_id])
    assert_df_equality(output_df, expected_df)
    assert all([a == b for a, b in zip(output_df.columns, expected_df.columns)])

def test_multijoin(spark_test_session):
    """
    Test MultiJoin
    """
    first       = spark_test_session.createDataFrame([{'id': 1, 'value': None}, {'id': 2, 'value': 2}])
    second      = spark_test_session.createDataFrame([{'id': 1, 'value': 1}, {'id': 2, 'value': 22}])
    expected_df = spark_test_session.createDataFrame([{'id': 1, 'value': 1}, {'id': 2, 'value': 2}])
    output_df   = multijoin([first, second], on='id', how='inner', coalesce=['value'])
    assert_df_equality(output_df, expected_df)
    assert all([a == b for a, b in zip(output_df.columns, expected_df.columns)])
