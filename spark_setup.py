from pyspark.sql import SparkSession


def init_spark():
    print("Setting up spark cluster")
    spark = SparkSession.\
        builder.master("local[*]").\
        appName("job_track").\
        getOrCreate()

    print("spark setup done")

    return spark

