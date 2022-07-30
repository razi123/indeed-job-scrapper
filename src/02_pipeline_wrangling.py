from pyspark.sql.functions import DataFrame
import re

# This notebook cleans up the dataframe and basic cleanups like
# non repeated elements in the column


def re_clean_syntax(col_value: str):
    """
    :param col_value: row by row all elements of the columns in dataframe
    :return: return unique column elements in the dataframe, non repetead
    """

    m = re.findall('(?<=\[\')[a-zA-Z0-9\s\.\&\-\@\/]+(?=\'\])', str(col_value))
    new_sting_set = list(set(m))

    if len(new_sting_set) == 0:
        new_sting_set = "--"

    return new_sting_set


def clean_pipeline(df: DataFrame):
    columns = df.columns
    for col in columns:
        temp = df[col].apply(lambda x: re_clean_syntax(x))
        df[col] = temp
    return df
