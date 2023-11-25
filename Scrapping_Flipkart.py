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
csv_file = open("flipkart_assignment_2.csv", "w", newline="", encoding="utf-8")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(["Rating","Review","Description"])
url = f'https://www.flipkart.com/apple-iphone-15-pro-black-titanium-128-gb/p/itm96f61fdd7e604?pid=MOBGTAGPJTMGZC9U&lid=LSTMOBGTAGPJTMGZC9U35JOZB&marketplace=FLIPKART&q=iphone+15+pro&store=tyy%2F4io&srno=s_1_5&otracker=search&otracker1=search&fm=Search&iid=43b14077-c247-496c-82d3-3b8d49b442af.MOBGTAGPJTMGZC9U.SEARCH&ppt=sp&ppn=sp&ssid=g6g0e4sac00000001696684356460&qH=c9de95b3b911a866'
driver.get(url)
try:
        element_reviews= driver.find_element(By.PARTIAL_LINK_TEXT, "reviews")
        element_present = EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "reviews"))
        WebDriverWait(driver, 20).until(element_present)
        element_reviews.click()
except TimeoutException as e:
    print("Error occured: {e}")
html = driver.page_source
soup = BeautifulSoup(html, "html.parser")
reviews = soup.find_all("div", {"class": "_2wzgFH"})
#print (reviews)
for review in reviews:
    try:
        rating = review.find("div",{"class":"_3LWZlK"}).text
    except:
        rating = "N/A"
    try:
        review = review.find("p", {"class": "_2-N8zT"}).text.strip()
    except:
        review = "N/A"
    try:
        description = review.find("div",{"class": "t-ZTKy"})
        for desc in description:
            try:
               lvl1 = desc.find("div")
               lvl2 = lvl1.find("div")
            except:
               lvl2 ="N/A"
        print(lvl2)

    except:
        description = "N/A"

    csv_writer.writerow([rating,review,description])
driver.quit()
csv_file.close()
    
