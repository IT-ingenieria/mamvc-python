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



def getting_client_status(clientid):
    url = "https://api.airtable.com/v0/appTBNNxCsYlnysyx/Licenses?filterByFormula=%28%7Bid%7D%20%3D%20%27{0}%27%29".format(clientid)    
    headers = {    
        'Authorization': "Bearer patMbA1wfFvmaZXLY.d6103ee44b44efcd93395574d29a88f401e1b95fa3102793bb7d60eab64cc8b8"
    }    
    response = requests.get(url, headers=headers)  
    airtable = response.json()    
    airtable_records = airtable['records']
    for x in airtable_records:
        airtable_id = x['id']
        airtable_fields = x['fields']        
        client = airtable_fields['client']
        status = airtable_fields['status']        
        return status

def getting_monitor_airtable(priority):
    f =   open('strings.json')
    internalstring = json.load(f)['key']
    license_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    url = "https://api.airtable.com/v0/applYR9VnQiCo0T4D/Monitor?filterByFormula=AND({status} = 'pending', {priority} = '" + priority + "')"
    url = requote_uri(url)   
    headers = {    
        'Authorization': "Bearer {0}".format(license_decoded)
    }    
    response = requests.get(url, headers=headers)    
    print(response.text)    
    return response.json()
    
def update_monitor_airtable(airtable_id,node_ip,status):
    f =   open('strings.json')
    internalstring = json.load(f)['key']
    license_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    url = "https://api.airtable.com/v0/applYR9VnQiCo0T4D/Monitor"    
    headers = {
        'Authorization': "Bearer {0}".format(license_decoded),
        'content-type': 'application/json'
    }    
    payload = json.dumps({"records":[{ "id": "{0}".format(airtable_id),
    "fields": {
    "node_ip": "{0}".format(node_ip),
    "status": "{0}".format(status),
    }
    }]})    
    response = requests.request("PATCH",url,headers=headers,data=payload)        
    return response.json()

### getting nodes and assigning job
def getting_nodes_airtable(airtable_jobid):
    f =   open('strings.json')
    internalstring = json.load(f)['key']
    license_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    url = "https://api.airtable.com/v0/applYR9VnQiCo0T4D/Nodes"    
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
        airtable_capacity = airtable_fields['capacity']
        airtable_ip = airtable_fields['ip']
        airtable_node_status = airtable_fields['status']
        print(str(airtable_jobs) + " = jobs | capacity =  " + str(airtable_capacity) + " ip: " + airtable_ip)
        if 'offline' in airtable_node_status:                    
            airtable_ip = "NoResourceAvailable"        
        if 'online' in airtable_node_status:        
            if airtable_capacity > airtable_jobs:            
                print()
                print("Assigning node!!")
                airtable_jobs = airtable_jobs + 1
                update_nodes_airtable(airtable_id,airtable_jobs,airtable_jobid)
                break           
            else:            
                airtable_ip = "NoResourceAvailable"            
    return airtable_ip


def update_nodes_airtable(airtable_id,jobs,airtable_jobid):
    f =   open('strings.json')
    internalstring = json.load(f)['key']
    license_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    url = "https://api.airtable.com/v0/applYR9VnQiCo0T4D/Nodes"    
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
clientid = "000001"
status = getting_client_status(clientid)

if 'online' in str(status):

    priority = "high"
    airtable = getting_monitor_airtable(priority)
    airtable_records = airtable['records']
    #### Processing High priority assets!!!!
    if '[]' not in str(airtable_records):
        for x in airtable_records:
            airtable_id = x['id']
            airtable_fields = x['fields']
            airtable_jobid = airtable_fields['job_id']            
            if 'node_ip' not in str(airtable_fields):            
                airtable_ip = getting_nodes_airtable(airtable_jobid)
                if airtable_ip == "NoResourceAvailable":
                    print("nodes are busy!!!!")
                else:
                    status= "node assigned"
                    update_monitor_airtable(airtable_id,airtable_ip,status)
    if '[]' in str(airtable_records):
        priority = "medium"
        airtable = getting_monitor_airtable(priority)
        airtable_records = airtable['records']
        #### Processing Medium priority assets!!!!
        if '[]' not in str(airtable_records):
            for x in airtable_records:
                airtable_id = x['id']
                airtable_fields = x['fields']
                airtable_jobid = airtable_fields['job_id']            
                if 'node_ip' not in str(airtable_fields):            
                    airtable_ip = getting_nodes_airtable(airtable_jobid)
                    if airtable_ip == "NoResourceAvailable":
                        print("nodes are busy!!!!")
                    else:
                        status= "node assigned"
                        update_monitor_airtable(airtable_id,airtable_ip,status)
    if '[]' in str(airtable_records):
        priority = "medium"
        airtable = getting_monitor_airtable(priority)
        airtable_records = airtable['records']
        #### Processing Medium priority assets!!!!
        if '[]' not in str(airtable_records):
            for x in airtable_records:
                airtable_id = x['id']
                airtable_fields = x['fields']
                airtable_jobid = airtable_fields['job_id']            
                if 'node_ip' not in str(airtable_fields):            
                    airtable_ip = getting_nodes_airtable(airtable_jobid)
                    if airtable_ip == "NoResourceAvailable":
                        print("nodes are busy!!!!")
                    else:
                        status= "node assigned"
                        update_monitor_airtable(airtable_id,airtable_ip,status)    
        if '[]' in str(airtable_records):
            priority = "low"
            airtable = getting_monitor_airtable(priority)
            airtable_records = airtable['records']
            #### Processing low priority assets!!!!
            if '[]' not in str(airtable_records):
                for x in airtable_records:
                    airtable_id = x['id']
                    airtable_fields = x['fields']
                    airtable_jobid = airtable_fields['job_id']            
                    if 'node_ip' not in str(airtable_fields):            
                        airtable_ip = getting_nodes_airtable(airtable_jobid)
                        if airtable_ip == "NoResourceAvailable":
                            print("nodes are busy!!!!")
                        else:
                            status= "node assigned"
                            update_monitor_airtable(airtable_id,airtable_ip,status)    
