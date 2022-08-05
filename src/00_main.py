import os
import spark_setup
from importlib import import_module

scrap_info_01 = import_module("01_scrap_job_information", package=None)
basic_cleanup_02 = import_module("02_pipeline_wrangling", package=None)
add_info_to_df = import_module("03_append_info", package=None)
set_schema = import_module("set_schema", package=None)
spark_setup = import_module("spark_setup", package=None)
save_files = import_module("save_files", package=None)

csv_path = '/Users/khazi/Documents/projects/indeed-job-scrapper/csv_files/'


def main():
    spark = spark_setup.init_spark()
    empty_rdd = spark_setup.empty_spark_df()

    os.chdir(os.path.dirname(__file__))
    raw_file_path = "../job_description_files/"
    job_position = ["Data Engineer", "Data Analyst", "Data Scientist", "Business Intelligence"]
    locations = ["Aachen", "Berlin", "Hamburg", "Dresden", "Leipzig", "Bremen", "Dresden", "Frankfurt", "Stuttgart"
                "Nürnberg", "Bonn", "Karlsruhe", "Freiburg", "München", "Erlangen", "Kaiserslautern", "Cottbus", "Heidelberg",
                "Darmstadt", "Erfurt", ]
    date_posted = 1
    source_website = "indeed"

    schema = set_schema.schema_define()
    df_empty = spark.createDataFrame([], schema)

    for position in job_position:
        temp_pos = position.split(" ")
        for location in locations:
            website = f"https://de.indeed.com/Jobs?q={temp_pos[0]}%20{temp_pos[1]}&l={location}&fromage={date_posted}"

            obj1 = scrap_info_01.Myjobs(website, raw_file_path)
            df_raw= obj1.browser_jobs()
            df_cleaned = basic_cleanup_02.clean_pipeline(df_raw)
            df = add_info_to_df.pos_loc_source(spark, df_cleaned, temp_pos, location, source_website)
            df_empty = df_empty.union(df)

    save_files.to_csv(csv_path, df_empty)
    print(df_empty)


if __name__ == '__main__':
    main()
