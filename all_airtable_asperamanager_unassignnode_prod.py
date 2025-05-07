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
from requests.utils import requote_uri
import base64


def getting_monitor_airtable():
    f =   open('strings.json')
    data = json.load(f)
    internalstring = data['key']
    license_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Monitor?filterByFormula=OR({status} = 'complete', {status} = 'technical review')"
    url = requote_uri(url)    
    headers = {    
        'Authorization': "Bearer {0}".format(license_decoded)
    }    
    response = requests.get(url, headers=headers)    
    print(response.text)    
    return response.json()
    

def update_monitor_airtable(airtable_id,status):
    f =   open('strings.json')
    data = json.load(f)
    internalstring = data['key']
    license_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Monitor"    
    headers = {
        'Authorization': "Bearer {0}".format(license_decoded),
        'content-type': 'application/json'
    }    
    payload = json.dumps({"records":[{ "id": "{0}".format(airtable_id),
    "fields": {
    "status": "{0}".format(status)
    }
    }]})    
    response = requests.request("PATCH",url,headers=headers,data=payload)   
    return response.json()

### getting nodes and assigning job
def getting_nodes_airtable(airtable_jobid):
    f =   open('strings.json')
    data = json.load(f)
    internalstring = data['key']
    license_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Nodes"    
    headers = {    
        'Authorization': "Bearer {0}".format(license_decoded)
    }    
    response = requests.get(url, headers=headers)    
    airtable = response.json()
    airtable_records = airtable['records']
    for x in airtable_records:
        airtable_id = x['id']
        airtable_fields = x['fields']
        airtable_jobs = airtable_fields['jobs']        
        if 'job_id' in airtable_fields:
            airtable_nodes_job_id = airtable_fields['job_id']            
            if str(airtable_nodes_job_id) == str(airtable_jobid):            
                airtable_jobs = airtable_jobs - 1
                airtable_nodes_job_id = ""
                update_nodes_airtable(airtable_id,airtable_jobs,airtable_nodes_job_id)

def update_nodes_airtable(airtable_id,jobs,airtable_jobid):
    f =   open('strings.json')
    data = json.load(f)
    internalstring = data['key']
    license_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Nodes"    
    headers = {
        'Authorization': "Bearer {0}".format(license_decoded),
        'content-type': 'application/json'
    }    
    payload = json.dumps({"records":[{ "id": "{0}".format(airtable_id),
    "fields": {
    "jobs": jobs,
    "job_id": airtable_jobid
    }
    }]})    
    response = requests.request("PATCH",url,headers=headers,data=payload)       
    return response.json()

##### MAIN CODE #############
airtable = getting_monitor_airtable()
airtable_records = airtable['records']
for x in airtable_records:
    airtable_id = x['id']
    airtable_fields = x['fields']
    airtable_jobid = airtable_fields['job_id']    
    getting_nodes_airtable(airtable_jobid)
    status= "node unassigned"
    update_monitor_airtable(airtable_id,status)
