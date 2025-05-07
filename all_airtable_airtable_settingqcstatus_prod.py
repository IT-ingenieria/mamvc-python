# importing libraries
import os
import shutil
import os.path
from os import path
import subprocess
import datetime
import uuid
import time
import requests
import json
from datetime import datetime, timedelta
import base64

def getting_cps_airtable():
    f =   open('strings.json')
    data = json.load(f)
    internalstring = data['key']
    key_decoded = base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")
    final_key = str(key_decoded).replace("'","")
    final_license = final_key[1:]
    url = "https://api.airtable.com/v0/appRP12VzK6IxjjDG/MAM?filterByFormula=%28%7BQC%20Status%20Date%7D%20%3D%20%27%27%29&view=QC%20Status"    
    headers = {    
        'Authorization': "Bearer {0}".format(final_license)
    }    
    response = requests.get(url, headers=headers)    
    #print(response.text)    
    return response.json()



def update_cps_airtable(airtable_id,airtable_asset_status,airtable_qc_status,dt_string):
    f =   open('strings.json')
    data = json.load(f)
    internalstring = data['key']
    key_decoded = base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")
    final_key = str(key_decoded).replace("'","")
    final_license = final_key[1:]
    url = "https://api.airtable.com/v0/appRP12VzK6IxjjDG/MAM"    
    headers = {
        'Authorization': "Bearer {0}".format(final_license),
        'content-type': 'application/json'
    }    
    payload = json.dumps({"records":[{ "id": "{0}".format(airtable_id),
    "fields": {
    "Asset Status": "{0}".format(airtable_asset_status),  
    "QC Status": "{0}".format(airtable_qc_status),
    "QC Status Date": dt_string
    }
    }]})    
    response = requests.request("PATCH",url,headers=headers,data=payload)    
    #print(response.text)    
    return response.json()



##### MAIN CODE #############

airtable = getting_cps_airtable()
airtable_records = airtable['records']


for x in airtable_records:

    airtable_id = x['id']
    airtable_fields = x['fields']
    airtable_video_filename = airtable_fields['Video Filename']
    print(airtable_video_filename)
    airtable_asset_status = airtable_fields['Asset Status']
    airtable_qc_status = airtable_fields['QC Status']


    # datetime object containing current date and time
    now = datetime.now()    
    four_hours_from_now = now + timedelta(hours=4)     
    dt_string = four_hours_from_now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    # Setting asset status based on qc status....
    if 'Approved' in airtable_qc_status and 'QC' in airtable_asset_status:
    
        airtable_asset_status = "QCA"
        update_cps_airtable(airtable_id,airtable_asset_status,airtable_qc_status,dt_string)
        
    if 'Rejected' in airtable_qc_status and 'QC' in airtable_asset_status:
    
        airtable_asset_status = "ESP"    
        update_cps_airtable(airtable_id,airtable_asset_status,airtable_qc_status,dt_string)
  