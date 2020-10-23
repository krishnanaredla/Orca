import pytest
from functools import partial
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

def _inject(cls, names):
    @pytest.fixture(autouse=True)
    def auto_injector_fixture(self, request):
        for name in names:
            setattr(self, name, request.getfixturevalue(name))

    cls.__auto_injector_fixture = auto_injector_fixture
    return cls


def auto_inject_fixtures(*names):
    return partial(_inject, names=names)

@pytest.fixture(scope='session')
def spark_test_session():
    spark = SparkSession.builder.getOrCreate()
    sqlContext = SQLContext(spark)
    yield sqlContext
    spark.stop()