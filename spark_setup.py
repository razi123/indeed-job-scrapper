from pyspark.sql import SparkSession


def init_spark():
    print("Setting up spark cluster")
    spark = SparkSession.\
        builder.master("local[*]").\
        appName("job_track").\
        getOrCreate()

    print("spark setup done")

    return spark


def empty_spark_df():
    spark = SparkSession. \
        builder.\
        appName("Empty_df"). \
        getOrCreate()

    empty_rdd = spark.sparkContext.emptyRDD()

    print("empty df spark setup done")

    return empty_rdd

