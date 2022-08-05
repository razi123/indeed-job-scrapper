from pyspark.sql.functions import DataFrame
import os


def to_csv(csv_path: str, df: DataFrame):
	if len(os.listdir(csv_path)) == 0:
		df.write.option("header", True)\
				.option("delimeter", ";")\
				.csv(csv_path+ "jobs_extract")
	else:
		df.write.save(path=csv_path, format='csv', mode='append', sep= ';')

