from pyspark.sql.functions import DataFrame
import os
import datetime as dt
from pyspark.sql import SparkSession


def to_csv(spark: SparkSession, csv_path: str, df: DataFrame):
	# csv_date = dt.datetime.today().strptime("%Y-%m-%d")
	if len(os.listdir(csv_path)) == 0:
		df.write.option("header", True)\
				.option("delimeter", ";")\
				.csv(csv_path+ "jobs_extract/" + "df_final.csv")
	else:
		spark.read.option("header", True)\
				.option("delimeter", ";")\
				.csv(csv_path + "jobs_extract/" + "df_final.csv") \
				.union(df)\
				.coalesce(1)\
				.write.option("header", True)\
				.option("delimeter", ";")\
				.mode("Overwrite")\
				.csv(csv_path + "jobs_extract/" + "df_final.csv")


def append_dataframe(output_path: str, df_empty: DataFrame):
	if os.path.exists(output_path+ "df_final"):
		pass
		#df_final = df_final.union(df_empty)
	else:
		pass


