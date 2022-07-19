import os
import re
import nltk

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By

import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, DateType, StringType, FloatType
from pyspark.sql import SparkSession
import spark_setup


class Myjobs:
    def __init__(self, website, raw_folder_path):
        self.website = website
        self.raw_folder_path = raw_folder_path

    def browser_jobs(self, spark: SparkSession):
        options = webdriver.ChromeOptions()
        # options.add_experimental_option("debuggerAddress", "localhost:8989")   # using existing chrome browser
        options.add_argument("--headless")                # runs without invoking the browser
        options.add_experimental_option("detach", True)
        options.add_argument("--disable-infobars")
        driver = webdriver.Chrome(options=options)
        driver.get(self.website)
        #driver.implicitly_wait(0.5)

        jobs = driver.find_elements(By.CSS_SELECTOR, '.jobsearch-ResultsList > li')
        dict_company = dict()
        dict_company["title"] = []
        dict_company["company"] = []
        dict_company["link"] = []
        for j in jobs:
            try:
                job_title = j.find_element(By.CLASS_NAME, "jobTitle").text
                company_name = j.find_element(By.CLASS_NAME, "companyName").text
                link = j.find_element(By.TAG_NAME, "a").get_attribute('href')
                dict_company["title"].append(job_title)
                dict_company["company"].append(company_name)
                dict_company["link"].append(link)

            except NoSuchElementException:
                print('not found')

        self.extract_job_description(driver, dict_company, self.raw_folder_path)
        self.create_dataframe(spark, self.raw_folder_path)

    @staticmethod
    def extract_job_description(driver: webdriver, job_dict: dict, file_path: str):
        for i in range(len(job_dict["link"])):
            load_job_link = job_dict["link"][i]
            company_name = str(job_dict["company"][i]).replace(' ', '_')
            driver.get(load_job_link)
            driver.implicitly_wait(0.5)
            job_discription = driver.find_element(By.ID, 'jobDescriptionText').text
            if os.path.exists(os.path.join(file_path, company_name)):
                os.remove(os.path.join(file_path,company_name))

            file_obj = open(os.path.join(file_path, str(company_name).strip() + '.txt'), 'w')
            file_obj.write(job_discription)

    @staticmethod
    def create_dataframe(spark: SparkSession, folder_path:str):
        de_skills = dict()
        de_skills["Programming"] = ["python", "java", "scala", "pyspark", "spark"]
        de_skills["Database"] = ["sql", "postgres sql", "mongodb"]
        de_skills["Frameworks"] = ["django", "kubernetes", "django", "airflow"]
        de_skills["Technologies"] = ["ci", "cd", "github", "github actions", "gitlab", "docker"]
        de_skills["Cloud"] = ["azure", "aws", "google cloud", "gcp", "gcloud"]
        de_skills["Extra"] = ["kanban", "jira", "confluence"]
        de_skills["Tools"] = ["tableau", "power bi", "qlik sense"]

        schema = StructType([
            StructField("Company", StringType(), True),
            StructField("Date", DateType(), True),
            StructField("Sprachen", StringType(), True),
            StructField("Salary", FloatType(), True),
            StructField("Location", StringType(), True),
            StructField("Programming", StringType(), True),
            StructField("Frameworks", StringType(), True),
            StructField("Technology", StringType(), True),
            StructField("Cloud", StringType(), True),
            StructField("Extra", StringType(), True),
            StructField("Tools", StringType(), True),
        ])

        spark.createDataFrame([], schema)


def main():
    raw_file_path = "job_description_files/"
    job_position = "Data Engineer"
    location = "Berlin"
    date_posted = 1
    website = f"https://de.indeed.com/Jobs?q=Data%20Engineer&l={location}&fromage={date_posted}"
    spark = spark_setup.init_spark()
    ip1 = Myjobs(website, raw_file_path)
    ip1.browser_jobs(spark)


if __name__ == '__main__':
    main()
