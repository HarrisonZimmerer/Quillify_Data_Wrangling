import base64
import functions_framework
import firebase_admin
from firebase_admin import firestore
from google.cloud import storage
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import time
import json
import re
import requests
import pandas as pd
import io
import numpy as np
from dateutil import parser
#ReadME
#Here is a censored vesrion of a california grant webscraper


# Application Default credentials are automatically created.
firebase_admin.initialize_app()
db = firestore.client()
# GCP storage client & bucket
storage_client = storage.Client()
bucket = storage_client.get_bucket("quillify-ai.appspot.com")
clinical_trial_terms = ["Clinical Trial Required", "Studies with Humans Required","CT Required"]
research_codes = ['R00', 'R01', 'R03', 'R21', 'R33', 'R34', 'R41', 'R42', 'R43', 'R44', 'R61', 'U01', 'U19', 'U24', 'U34','UG1', 'UG3', 'UH3', 'UM1', 'DP1', 'DP2', 'SBIR', 'STTR', 'K01', 'K08', 'K12', 'K18', 'K22', 'K23', 'K24', 'K25', 'K76', 'K99', 'F31', 'F32', 'F99', 'K00', 'P01', 'P20', 'P30', 'P50', 'U54', 'T32', 'RC2', 'G11', 'SB1', 'R15', 'R25', 'R36', 'R50', 'R61', 'R33', 'RM1', 'U54', 'R18', 'R24', 'R25', 'R38', 'R60', 'U13', 'U18', 'U2C', 'U42', 'U44', 'F33', 'P40', 'P41', 'P51']
# Precompile regular expressions for clinical trial terms and research codes.
clinical_trial_pattern = re.compile('|'.join(map(re.escape, clinical_trial_terms)), re.IGNORECASE)
research_codes_pattern = re.compile('|'.join(map(re.escape, research_codes)), re.IGNORECASE)
def check_clinical_trial_required(title):
    return bool(clinical_trial_pattern.search(title))
def check_is_research_grant(title,program):
    return program=="SBIR" or bool(research_codes_pattern.search(title))
# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def parse_grants(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    df = download_and_upload_to_gcs()
    df = data_wrangle(df)
    for index,row in df.apply(extract_data,axis=1).iterrows():
      name = re.sub("/","++", str(row['solicitation_number']) + "_" + str(row["topic_number"]))
      if checkUniqueness(name,row):
        sendPubSubEncodeTrigger(json.dumps(row))
      else:
        continue
      if (index+1) % 25 == 0:
        time.sleep(1)
      else:
        time.sleep(0.25)
    print("COMPLETE",index)
def checkUniqueness(name,row):
   # Check if this already exists, if so, skip instead of adding a new one every time.
    for doc in db.collection("grantinfo").where("gcspath","==",f"grant_embeddings/{name}_embeddings.json").limit(1).stream():
        ddict = doc.to_dict()
        last_updated_time = datetime.strptime(row['last_updated_date'], '%m%d%Y')
        if "last_updated_date" not in ddict:
          db.collection("grantinfo").document(doc.id).update({"last_updated_date": last_updated_time})
        if last_updated_time > ddict.get("last_updated_date",ddict.get('dateCreated',datetime.now().replace(tzinfo=None)).replace(tzinfo=None)).replace(tzinfo=None):
          return True
        else:
          return False
    return True
def update_applicant_type_phrasing(applicant_type, applicant_type_dict): #turn the string applicant types into a proper list
    #iterate over each part of the collum separating the string into a list
    #for applicant_types in df['ApplicantType']: #get string
    placeholder_list = []
    for applicant_type in applicant_type.split("; "):  # Assuming applicant_type is a string separated by "; "
        codes = applicant_type_dict.get(applicant_type, [])
        if codes:  # Only concatenate if codes is not empty
            placeholder_list += codes
    return placeholder_list
def update_applicant_type_code(applicant_type, applicant_type_code_dict): #turn the string applicant types into a proper list
    #iterate over each part of the collum separating the string into a list
    #for applicant_types in df['ApplicantType']: #get string
    placeholder_list = []
    for applicant_type in applicant_type.split("; "):  # Assuming applicant_type is a string separated by "; "
        codes = applicant_type_code_dict.get(applicant_type, [])
        if codes:  # Only concatenate if codes is not empty
            placeholder_list += codes
    return placeholder_list
def clean_date_string(date_string, phrases_to_remove, phrases_to_replace):
    if isinstance(date_string, float) and np.isnan(date_string):
        return date_string
    if isinstance(date_string, float):  # Convert to string if not NaN
        date_string = str(date_string)
    for phrase, replacement in phrases_to_replace.items():  # Replace the phrases
        date_string = re.sub(r'\b' + re.escape(phrase) + r'\b', replacement, date_string)
    for phrase in phrases_to_remove:  # Remove unwanted phrases
        date_string = date_string.replace(phrase, '')
    date_string = re.sub(r'\s+', ' ', date_string).strip()  # Normalize whitespace
    return date_string

def transform_date(date_string, phrases_to_remove, phrases_to_replace):
    if isinstance(date_string, float) and np.isnan(date_string):
        return None
    try:
        date_string = clean_date_string(date_string, phrases_to_remove, phrases_to_replace)  # Clean the date string
        parsed_date = parser.parse(date_string, fuzzy=True)  # Parse the date
        return parsed_date.strftime('%m%d%Y')  # Format the date as %m%d%Y
    except (ValueError, TypeError):
        return None
def data_wrangle(df):
    df['formatted_dates'] = df['ExpAwardDate'].apply(lambda x: transform_date(x, phrases_to_remove, phrases_to_replace))# Applying the transform_date function to the 'ExpAwardDate' column
    df['Categories'] = df['Categories'].apply(lambda x: [category_dict.get(y,y) for y in x.split("; ")])
    df['CategoryCodes'] = df['Categories'].apply(lambda x: [category_of_funding_activity_code_dict.get(y,'O') for y in x])
    df['ApplicantType'] = df['ApplicantType'].fillna("Unrestricted")
    df['ApplicantTypes'] = df['ApplicantType'].apply(lambda x: update_applicant_type_phrasing(x, applicant_type_dict))
    df['ApplicantTypeCode'] = df['ApplicantType'].apply(lambda x: update_applicant_type_code(x, applicant_type_code_dict))
    df['Type_code'] = df['Type'].apply(lambda x: [contract_types_dict.get(y,y) for y in x.split("; ")])
    df[['floor', 'ceiling']] = df['EstAmounts'].apply(extract_floor_ceiling)
    df["GrantURL"] = df["GrantURL"].fillna(df["AgencyURL"])
    return df
def extract_data(row):
    data= {}
    '''Data wrangle'''
    return data
def extract_floor_ceiling(amount_str):
    # Find all amounts in the string
    amounts = re.findall(r'\$[\d,]+', amount_str.replace(',', ''))
    amounts = [int(a.replace('$', '').replace(',', '')) for a in amounts]
    if len(amounts) == 2:
        floor, ceiling = amounts
    elif len(amounts) == 1:
        floor, ceiling = 0, amounts[0]
    else:
        floor, ceiling = 0, 0
    return pd.Series({'floor': floor, 'ceiling': ceiling})
def sendPubSubEncodeTrigger(rowjson):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("quillify-ai", "encode-document")
    publisher.publish(topic_path, data=rowjson.encode("utf-8"))
    return 'Message published to topic.', 200
def download_and_upload_to_gcs():
    # Define the URL of the file to download
    url = "url"
    # Download the file and upload it to GCS
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code == 200:
        # Generate the new file name
        new_file_name = "california-grants-portal-data.csv"
        # Create a new blob with the generated file name
        blob = bucket.blob(new_file_name)
        # Upload the CSV content to GCS
        blob.upload_from_string(response.content, content_type='text/csv')
        # Convert the CSV content to a pandas DataFrame
        return pd.read_csv(io.StringIO(response.text))
    else:
        raise ValueError(f'Failed to download file with status code: {response.status_code}')
    
category_dict = {
    ''' category'''
    }
category_of_funding_activity_code_dict = {
    '''funding activity code'''
}
applicant_type_dict = {
    '''applicant type'''
}
applicant_type_code_dict = {
    '''applicant code'''
}
contract_types_dict = {
    '''type '''
}
phrases_to_remove = [
    '''phrases to remove'''
]

phrases_to_replace = {
    '''phrases to replace'''
}