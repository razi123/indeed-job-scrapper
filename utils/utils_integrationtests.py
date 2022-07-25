import inspect
import os
import sys
from pyspark.sql.types import *
from pyspark.sql import DataFrame

import spark_setup as ss

spark = ss.init_spark()


class Error(Exception):
    """ Base class for other exceptions"""
    pass


class DataFrameNotEqualError(Error):
    """Raised when expected datafrmae and expected dataframe are not same """
    pass


def create_spark_df_from_dict(input_dict: dict, col_types=None):

    column_names, data = zip(*input_dict.items())
    if col_types:
        schema = []

        for i, col_name in enumerate(column_names):
            schema.append(StructField(col_name, col_types[i], True))
        return spark.createDataFrame(zip(*data), schema=StringType(schema))
    else:
        return spark.createDataFrame(zip(*data), column_names)


def assert_equal_df(df_expected: DataFrame, df_observed: DataFrame):
    """

    :param df_expected:
    :param df_observed:
    :return:
    """

    diff1 = df_expected.subtract(df_observed)
    diff2 = df_observed.subtract(df_expected)
    test_failed =  False

    if diff1.count() > 0:
        print("Expected but not observed")
        diff1.orderBy(diff1.columns).show(truncate=False)
        test_failed = True

    if diff2.count() > 0:
        print("Observed but not expected")
        diff2.orderBy(diff2.columns).show(truncate=False)
        test_failed= True

    if df_expected.columns != df_observed.columns:
        print("Columns not same")
        print("Expected columns = " + str(df_expected.columns))
        print("Observed columns = " + str(df_observed.columns))
        test_failed = True

    if test_failed:
        print(f"{inspect.stack()[1].function} - Test fails")
        raise DataFrameNotEqualError("DataFrames are not equal")
    else:
        print(f"{inspect.stack()[1].function} - Test passed")