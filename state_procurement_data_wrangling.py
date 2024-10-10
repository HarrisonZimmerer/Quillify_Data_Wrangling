from dateutil import parser
from datetime import datetime
import re
import requests
import pandas as pd
import io
from io import BytesIO
import numpy as np
import urllib.parse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import urllib.parse

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
df = pd.read_html('./ps.xls')[0]


def convert_date(date_str):
    if date_str is "Expired":
        return "Expired"  # Handle None values in the DataFrame

    # Remove the time zone abbreviation using regex
    date_str_cleaned = re.sub(r'\s[A-Z]{2,4}$', '', date_str)
    # Define the input format without time zone
    input_format = "%m/%d/%Y %I:%M%p"
    # Define the output format
    output_format = "%m%d%Y"
    # Convert the string to a datetime object and format it
    dt = datetime.strptime(date_str_cleaned, input_format)
    formatted_date = dt.strftime(output_format)
    return formatted_date

def remove_passed_dates(date_str, date_format="%m/%d/%Y %I:%M%p"):
    # Get the current date and time
    now = datetime.now()
    date_str_cleaned = re.sub(r'\s[A-Z]{2,4}$', '', date_str)
    # Parse the input date string into a datetime object
    try:
        date_to_check = datetime.strptime(date_str_cleaned, date_format)
        
        # Compare the input date with the current date
        if date_to_check < now:
            return "Expired"
        else:
            return date_to_check
    except ValueError as e:
        print(f"Error parsing date: {e}")
        return None

def updateDataFrameURL(dataframe):
    baseurl = "https://caleprocure.ca.gov/event/"
     
    #tempurl = baseurl + Department + "/" + 

    def create_department_url(department_no_df):
        department_no = urllib.parse.quote(str(department_no_df))
        return baseurl + department_no
    
    dataframe['tempurl'] = dataframe['Department'].apply(create_department_url)
    dataframe['url'] = df['tempurl'] + '/' + df['Event ID']
    dataframe.drop(['tempurl'], axis='columns')
    dataframe = dataframe.drop(['tempurl'], axis='columns')
    return dataframe

def getDescFromWebsite(url):
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    driver.get(url)  
    time.sleep(6)
    element = driver.find_element(By.XPATH, "/html/body/div[8]/div/div/div[2]/div[2]/form/div[3]/section[1]/div[2]/div/div")
    text = element.text
    driver.quit()
    return text

def updateDataFrameOneLiner(description):
    return description.split('.')[0] + '.'

def dataRename(row):
    data= {} 
    
    '''Program for entering the Data into database'''

    return data

df['formatted_date'] = df['End Date'].apply(convert_date)
df['End Date'] = df['End Date'].apply(remove_passed_dates)
df = df[df['End Date'] != 'Expired']
df = updateDataFrameURL(df)
df['Description'] = df['url'].apply(getDescFromWebsite)
df['One Liner'] = df['Description'].apply(updateDataFrameOneLiner)
df = df.apply(dataRename,axis=1).apply(pd.Series).head()