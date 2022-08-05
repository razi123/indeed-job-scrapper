import re
from pyspark.sql.types import StringType, StructField, StructType
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from importlib import import_module

setup_spark = import_module("spark_setup", package=None)
spark = setup_spark.init_spark()


def contact_email(job_desc: str):
    mail_id = re.findall(f"[\w\.-]+@[\w\.-]+", str(job_desc))
    if len(mail_id) == 0:
        email = " -- "
    else:
        email = mail_id

    return email


def pos_loc_source(spark: SparkSession, df, pos: str, loc: str, src: str):
    schema = StructType([
        StructField("Date", StringType(), True), \
        StructField("Company", StringType(), True), \
        StructField("Programming", StringType(), True), \
        StructField("Database", StringType(), True), \
        StructField("Frameworks", StringType(), True), \
        StructField("Technologies", StringType(), True), \
        StructField("Cloud", StringType(), True), \
        StructField("Extras", StringType(), True), \
        StructField("Tools", StringType(), True), \
        StructField("Sprachen", StringType(), True), \
        StructField("Email", StringType(), True), \
    ])

    df_spark = spark.createDataFrame(df, schema)

    df_spark = df_spark. \
        withColumn("Position", f.lit(str(pos))). \
        withColumn("Location", f.lit(str(loc))). \
        withColumn("Website", f.lit(str(src))). \
        select("Date", "Position", "Location", "Company", "Programming", "Database", "Frameworks", "Technologies",
               "Cloud", "Extras", "Tools", "Sprachen", "Email", "Website")

    return df_spark










