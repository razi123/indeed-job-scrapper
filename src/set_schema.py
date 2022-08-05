from pyspark.sql.types import StringType, StructField, StructType


def schema_define():
	schema = StructType([
		StructField("Date", StringType(), True), \
		StructField("Position", StringType(), True), \
		StructField("Location", StringType(), True), \
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
		StructField("Website", StringType(), True),
		])

	return schema