import os
import re
import pandas as pd
import datetime
from importlib import import_module

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By

from pyspark.sql import SparkSession

skills_defined_100 = import_module("100_define_skillset", package=None)
append_info = import_module("03_append_info", package=None)


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

        df = self.extract_job_description(driver, dict_company, self.raw_folder_path)

        return df


    @staticmethod
    def extract_job_description(driver: webdriver, job_dict: dict, file_path: str):
        de_skills = skills_defined_100.define_skills()
        keys = list(de_skills.keys())
        df_empty = pd.DataFrame(columns=["Date", "Company", "Programming", "Database", "Frameworks", "Technologies", "Cloud",
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

            #n = append_info.contact_email(job_discription)

            out_dict = {"Date": [], "Company": [], "Programming": [], "Database": [], "Frameworks": [], "Technologies": [],
                        "Cloud": [], "Extras": [], "Tools": [], "Sprachen": [],}
            out_dict["Company"].append(job_dict["company"][i])
            out_dict["Date"].append(datetime.datetime.today().date().strftime("%m-%d-%Y"))
            #out_dict["Email"].append(n)

            for idx, value in enumerate(de_skills.values()):
                for element in value:
                    try:
                        m = re.findall(f'(?<=[\s\(]){element}(?=[\s\)\.])', str(job_discription))
                        if m:
                            out_dict[keys[idx]].append(m)

                    except AttributeError:
                        m = re.search(f'(?<=[\s\(]){element}(?=\s)', str(job_discription))

            df_empty = pd.concat([
                df_empty, pd.DataFrame.from_records([{
                    'Date': out_dict["Date"],
                    'Company': out_dict["Company"],
                    'Programming': out_dict["Programming"],
                    'Database': out_dict["Database"],
                    'Frameworks': out_dict["Frameworks"],
                    'Technologies': out_dict["Technologies"],
                    'Cloud': out_dict["Cloud"],
                    'Extras': out_dict["Extras"],
                    'Tools': out_dict["Tools"],
                    'Sprachen': out_dict["Sprachen"],
                }])])

        return df_empty
