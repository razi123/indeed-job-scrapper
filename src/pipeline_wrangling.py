from pyspark.sql.functions import DataFrame
from pyspark.sql.functions import StringType
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

import re
import pandas as pd



def re_clean_again(col_value: str):
    """    if len(col_value > 1):
        n = re.findall(f'(?<=\')[a-zA-Z]+(?=\')')
    else:"""
    n = re.findall(f'(?<=\')[a-zA-Z0-9\s]+(?=\')', str(col_value))
    return n


def re_clean_syntax(col_value: str):
    m = re.findall('(?<=\[\')[a-zA-Z0-9\s\.\&]+(?=\'\])', str(col_value))
    #m = re.split("'", str(col_value))
    #for i in range(len(m)):
    #    if i%2 != 0:
    new_sting_set = list(set(m))

    if len(new_sting_set) == 0:
        new_sting_set = "--"

    return new_sting_set


def clean_pipeline(df: DataFrame):
    columns = df.columns
    for col in columns:
        #df = df.filter(f.col(col).rlike("(?<=\[')[a-zA-Z0-9\s\.\&]+(?=\'])"))
        temp = df[col].apply(lambda x: re_clean_syntax(x))
        df[col] = temp
        #df[col] = df[col].apply(lambda y: re_clean_again(y))
    return df
