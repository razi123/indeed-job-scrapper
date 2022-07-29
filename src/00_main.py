import os
import spark_setup
from importlib import import_module

scrap_info_01 = import_module("01_scrap_job_information", package=None)
basic_cleanup_02 = import_module("02_pipeline_wrangling", package=None)
add_info_to_df = import_module("03_append_info", package=None)


def main():
    os.chdir(os.path.dirname(__file__))

    raw_file_path = "../job_description_files/"
    job_position = "Data Engineer"
    location = "Berlin"
    date_posted = 1
    website = f"https://de.indeed.com/Jobs?q=Data%20Engineer&l={location}&fromage={date_posted}"
    spark = spark_setup.init_spark()
    obj1 = scrap_info_01.Myjobs(website, raw_file_path)
    df_raw= obj1.browser_jobs(spark)
    df_cleaned = basic_cleanup_02.clean_pipeline(df_raw)
    print(df_cleaned)


if __name__ == '__main__':
    main()
