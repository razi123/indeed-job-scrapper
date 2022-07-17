import os

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select


class Myjobs:
    def __init__(self, website):
        self.website = website

    def browser_jobs(self):
        options = webdriver.ChromeOptions()
        # options.add_experimental_option("debuggerAddress", "localhost:8989")   # using existing chrome browser
        options.add_argument("--headless")                # runs without invoking the browser
        options.add_experimental_option("detach", True)
        options.add_argument("--disable-infobars")
        driver = webdriver.Chrome(options=options)
        driver.get(self.website)
        driver.implicitly_wait(0.5)

        jobs = driver.find_elements(By.CSS_SELECTOR, '.jobsearch-ResultsList > li')
        dict_company = dict()
        dict_company["title"] = []
        dict_company["company"] = []
        dict_company["link"] = []
        for j in jobs:
            try:
                job_title = j.find_element(By.CLASS_NAME, "jobTitle").text
                company_name = j.find_element(By.CLASS_NAME, "companyName").text
                link = jobs[1].find_element(By.TAG_NAME, "a").get_attribute('href')
                dict_company["title"].append(job_title)
                dict_company["company"].append(company_name)
                dict_company["link"].append(link)

            except NoSuchElementException:
                print('not found')

        self.extract_job(driver, dict_company)

    @staticmethod
    def extract_job(driver: webdriver, job_dict: dict):
        load_job_link = job_dict["link"][0]
        driver.get(load_job_link)
        driver.implicitly_wait(0.5)

        job_discription = driver.find_elements(By.CSS_SELECTOR, '.jobDescriptionText > ul')
        print(job_discription)
        for j in job_discription:
            print(j)









def main():
    job_position = "Data Engineer"
    location = "Berlin"
    date_posted = 1
    website = f"https://de.indeed.com/Jobs?q=Data%20Engineer&l={location}&fromage={date_posted}"
    ip1 = Myjobs(website)
    ip1.browser_jobs()


if __name__ == '__main__':
    main()
