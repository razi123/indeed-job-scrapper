from pyspark.sql.functions import DataFrame
import re


def re_clean_again(col_value: str):
    """    if len(col_value > 1):
        n = re.findall(f'(?<=\')[a-zA-Z]+(?=\')')
    else:"""
    n = re.findall(f'(?<=\')[a-zA-Z0-9\s]+(?=\')', str(col_value))
    return n


def re_clean_syntax(col_value: str):
    m = re.findall('(?<=\[\')[a-zA-Z0-9\s\.\&]+(?=\'\])', str(col_value))

    return m


def clean_pipeline(df: DataFrame):
    columns = df.columns
    for col in columns:
        df[col] = df[col].apply(lambda x: re_clean_syntax(x))
        #df[col] = df[col].apply(lambda y: re_clean_again(y))

    return df
