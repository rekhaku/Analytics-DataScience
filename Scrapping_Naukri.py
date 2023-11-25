import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import csv

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
csv_file = open("naukri_assignment_1.csv", "w", newline="", encoding="utf-8")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(["Title", "Company", "Experience"])
for i in range(1, 11):
    url = f'https://www.naukri.com/remote-jobs-{i}?src=gnbjobs_homepage_srch/'
    driver.get(url)
    try:
        element_present = EC.presence_of_element_located((By.CLASS_NAME, "cust-job-tuple"))
        WebDriverWait(driver, 20).until(element_present)
    except TimeoutException as e:
        print("Error occured: {e}")
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    job_posts = soup.find_all("div", {"class": "cust-job-tuple"})
    for job in job_posts:
        try:
            job_title = job.find("a",{"class":"title"}).text
        except:
            job_title = "N/A"

        try:
            company_name = job.find("a", {"class": "comp_name"}).text
        except:
            company_name = "N/A"

        try:
            exp = job.find("span", {"class": "expwdth"}).text
        except:
            exp = "N/A"

        csv_writer.writerow([job_title,company_name,exp])


driver.quit()
csv_file.close()