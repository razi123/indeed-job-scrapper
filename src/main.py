import os
import re
import pandas as pd

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
import spark_setup
import pipeline_wrangling


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

        df_schema = StructType([
            StructField("Company", StringType(), nullable=True),
            StructField("Programming", StringType(), nullable=True),
            StructField("Database", StringType(), nullable=True),
            StructField("Frameworks", StringType(), nullable=True),
            StructField("Technologies", StringType(), nullable=True),
            StructField("Cloud", StringType(), nullable=True),
            StructField("Extras", StringType(), nullable=True),
            StructField("Tools", StringType(), nullable=True),
            StructField("Sprachen", StringType(), nullable=True),
        ])

        df = self.extract_job_description(spark, driver, dict_company, self.raw_folder_path)
        #df2 = df.iloc[:, :-1]  # check why extra named column is added at hte end of column
        #df = spark.createDataFrame(df2, schema=df_schema)
        df_cleaned = pipeline_wrangling.clean_pipeline(df)


    @staticmethod
    def extract_job_description(spark: SparkSession, driver: webdriver, job_dict: dict, file_path: str):
        de_skills = dict()
        de_skills["Programming"] = ["python", "java", "javascript", "scala", "pyspark", "spark", "react", "html5", "css",
                                    "typescript", "nextjs", "nodejs", "rust", ]
        de_skills["Database"] = ["sql", "postgresql", "mongodb", "nosql", "ElasticSearch", "RabbitMQ"]
        de_skills["Frameworks"] = ["django", "kubernetes", "django", "airflow"]
        de_skills["Technologies"] = ["ci/ci", "github", "github actions", "gitlab", "docker"]
        de_skills["Cloud"] = ["azure", "aws", "google cloud", "gcp", "gcloud"]
        de_skills["Extras"] = ["kanban", "jira", "confluence"]
        de_skills["Tools"] = ["tableau", "power bi", "qlik sense"]
        de_skills["Sprachen"] = ["Englisch", "English", "Deutsch", "German"]


        keys = list(de_skills.keys())
        values = list(de_skills.values())

        df_empty = pd.DataFrame(columns=["Company", "Programming", "Database", "Frameworks", "Technologies", "Cloud",
                                         "Extras", "Tools", "Sprachen"])

        for i in range(len(job_dict["link"])):
            load_job_link = job_dict["link"][i]
            company_name = str(job_dict["company"][i]).replace(' ', '_')
            driver.get(load_job_link)
            driver.implicitly_wait(0.5)
            job_discription = driver.find_element(By.ID, 'jobDescriptionText').text
            job_discription = job_discription.lower()
            if os.path.exists(os.path.join(file_path, company_name)):
                os.remove(os.path.join(file_path, company_name))

            company_name = re.sub(r"[/\s]+", "", company_name)
            file_obj = open(os.path.join(file_path, str(company_name) + '.txt'), 'w')
            file_obj.write(job_discription)

            out_dict = {"Company": [], "Programming": [], "Database": [], "Frameworks": [], "Technologies": [],
                        "Cloud": [], "Extras": [], "Tools": [], "Sprachen": [], }
            out_dict["Company"].append(job_dict["company"][i])

            for idx, value in enumerate(de_skills.values()):
                for element in value:
                    try:
                        m = re.findall(f'(?<=[\s\(]){element}(?=[\s\)\.])', str(job_discription))
                        if m:
                            out_dict[keys[idx]].append(m)

                    except AttributeError:
                        m = re.search(f'(?<=[\s\(]){element}(?=\s)', str(job_discription))

            df_empty = pd.concat([df_empty, pd.DataFrame.from_records([{'Company': out_dict["Company"], 'Programming': out_dict["Programming"],
                             'Database': out_dict["Database"], 'Frameworks': out_dict["Frameworks"],
                             'Technologies': out_dict["Technologies"], 'Cloud': out_dict["Cloud"],
                             'Extras': out_dict["Extras"], 'Tools': out_dict["Tools"],
                             'Sprachen': out_dict["Sprachen"],}])])

        return df_empty


def main():
    os.chdir(os.path.dirname(__file__))

    raw_file_path = "../job_description_files/"
    job_position = "Data Engineer"
    location = "Berlin"
    date_posted = 1
    website = f"https://de.indeed.com/Jobs?q=Data%20Engineer&l={location}&fromage={date_posted}"
    spark = spark_setup.init_spark()
    ip1 = Myjobs(website, raw_file_path)
    ip1.browser_jobs(spark)


if __name__ == '__main__':
    main()
