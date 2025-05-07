# importing libraries
import os
import shutil
import subprocess
import time
import requests
import json
import base64


def auth_airtable(key):
    url = "https://api.airtable.com/v0/appTBNNxCsYlnysyx/Licenses?filterByFormula=%28%7Bid%7D%20%3D%20%27000001%27%29"
    headers = {
    
        'Authorization': "Bearer {0}".format(key)
    }    
    response = requests.get(url, headers=headers)
    license = response.json()['records'][0]['fields']['key']       
    return license


def getting_monitor_airtable():
    f =   open('strings.json')
    internalstring = json.load(f)['internalstring']
    key_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    license = auth_airtable(key_decoded)
    license_decoded = str(base64.b64decode(f"{auth_airtable(key_decoded)}{'=' * (4 - len(auth_airtable(key_decoded)) % 4)}")).replace("'","")[1:]    
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Monitor?filterByFormula=%28%7Bstatus%7D%20%3D%20%27node%20assigned%27%29"    
    headers = {    
        'Authorization': "Bearer {0}".format(license_decoded)
    }    
    response = requests.get(url, headers=headers)    
    #print(response.text)    
    return response.json()
    

def update_monitor_airtable(airtable_id,asset_status):
    f =   open('strings.json')
    internalstring = json.load(f)['internalstring']
    key_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    license = auth_airtable(key_decoded)
    license_decoded = str(base64.b64decode(f"{auth_airtable(key_decoded)}{'=' * (4 - len(auth_airtable(key_decoded)) % 4)}")).replace("'","")[1:]    
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Monitor"    
    headers = {
        'Authorization': "Bearer {0}".format(license_decoded),
        'content-type': 'application/json'
    }    
    payload = json.dumps({"records":[{ "id": "{0}".format(airtable_id),
    "fields": {
    "status": "{0}".format(asset_status),
    }
    }]})    
    response = requests.request("PATCH",url,headers=headers,data=payload)  
    return response.json()



def update_monitor_airtable_error(airtable_id,error_message):
    f =   open('strings.json')
    internalstring = json.load(f)['internalstring']
    key_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    license = auth_airtable(key_decoded)
    license_decoded = str(base64.b64decode(f"{auth_airtable(key_decoded)}{'=' * (4 - len(auth_airtable(key_decoded)) % 4)}")).replace("'","")[1:]    
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Monitor"    
    headers = {
        'Authorization': "Bearer {0}".format(license_decoded),
        'content-type': 'application/json'
    }    
    payload = json.dumps({"records":[{ "id": "{0}".format(airtable_id),
    "fields": {
    "error message": "{0}".format(error_message)
    }
    }]})    
    response = requests.request("PATCH",url,headers=headers,data=payload)
    return response.json()



def update_monitor_airtable_progress(airtable_id,percentage,bytes_sent,speed,eta):
    f =   open('strings.json')
    internalstring = json.load(f)['internalstring']
    key_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    license = auth_airtable(key_decoded)
    license_decoded = str(base64.b64decode(f"{auth_airtable(key_decoded)}{'=' * (4 - len(auth_airtable(key_decoded)) % 4)}")).replace("'","")[1:]    
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Monitor"    
    headers = {
        'Authorization': "Bearer {0}".format(license_decoded),
        'content-type': 'application/json'
    }    
    payload = json.dumps({"records":[{ "id": "{0}".format(airtable_id),
    "fields": {
    "percentage": "{0}".format(percentage),
    "bytes sent": "{0}".format(bytes_sent),
    "speed": "{0}".format(speed),
    "eta": "{0}".format(eta)
    }
    }]})    
    response = requests.request("PATCH",url,headers=headers,data=payload)    
    return response.json()


def getting_profiles_airtable(preset_name,input_file,airtable_id):
    f =   open('strings.json')
    internalstring = json.load(f)['internalstring']
    key_decoded = str(base64.b64decode(f"{internalstring}{'=' * (4 - len(internalstring) % 4)}")).replace("'","")[1:]
    license = auth_airtable(key_decoded)
    license_decoded = str(base64.b64decode(f"{auth_airtable(key_decoded)}{'=' * (4 - len(auth_airtable(key_decoded)) % 4)}")).replace("'","")[1:]    
    url = "https://api.airtable.com/v0/app8K2BkSQFAvLS6a/Profiles?filterByFormula=%28%7Bprofile%20name%7D%20%3D%20%27{0}%27%29".format(preset_name)    
    headers = {    
        'Authorization': "Bearer {0}".format(license_decoded)
    }    
    response = requests.get(url, headers=headers)    
    # variables from datamover settings
    datamover_settings = response.json()
    datamover_settings_presets = datamover_settings['records']
    #print(transcode_settings_presets)    
    # getting video filename without extension      
    input_file_mod = input_file.split(".")
    input_file_str = input_file_mod[0]
    count = 1    
    for preset in datamover_settings_presets:
        preset_id = preset['id']
        presets_fields = preset['fields']
        profile_name = presets_fields['profile name']
        delivery_method = presets_fields['delivery method']
        replace = presets_fields['replace']
        source_path = presets_fields['source path']
        output_path = presets_fields['output path']    
        aspera_user = presets_fields['remote user']
        aspera_password = presets_fields['remote password']        
        aspera_ip = presets_fields['remote ip']            


        # building input full path and output full path...
        input_fullpath = source_path + input_file                      
        transfer_command = "ascp -T --policy=fair -l 100m -P 33001 " + " " + input_fullpath + " " + aspera_user +  "@"  + aspera_ip + ":" + output_path

        try:
            trigger_copy_commands(airtable_id,transfer_command)                    
        except:
            input_fullpath = "Could not complete"
                
    return input_fullpath
     


def trigger_copy_commands(airtable_id,transfer_command):
        
    asset_status = "in progress"
    update_monitor_airtable(airtable_id,asset_status)        
    command = transfer_command.split()    
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # Capture stdout and stderr in real-time
    for stdout_line in iter(process.stdout.readline, ''):
        time.sleep(5)
        #print(stdout_line, end='')  # Print stdout (video/audio transcoding information)
        line = str(stdout_line).split()
        #print(line)
        if len(line)>0:
            filename = line[0]
            percentage = line[1]
            bytes_sent = line[2]
            speed = line[3]
            try:
                eta = line[4]
            except:
                eta = ""
            print(filename,percentage,bytes_sent,speed,eta)
            update_monitor_airtable_progress(airtable_id,percentage,bytes_sent,speed,eta)
    for stderr_line in iter(process.stderr.readline, ''):
        print(stderr_line, end='')  # Print stderr (progress, error messages, etc.)
        error_message = stderr_line
        update_monitor_airtable_error(airtable_id,error_message)
        
    # Close the streams and wait for the process to finish
    process.stdout.close()
    process.stderr.close()
    process.wait()
    


##### MAIN CODE #############
airtable = getting_monitor_airtable()
airtable_records = airtable['records']
for x in airtable_records:
    airtable_id = x['id']
    airtable_fields = x['fields']
    airtable_filename = airtable_fields['filename']
    airtable_node_ip = airtable_fields['node_ip']
    airtable_datamover_profiles = airtable_fields['Datamover Profiles']
    if str(airtable_node_ip) == "192.168.24.52":    
        for preset_name in airtable_datamover_profiles:
            input_fullpath = getting_profiles_airtable(preset_name,airtable_filename,airtable_id)               
        if "Could not complete" in input_fullpath:
            asset_status = "technical review"
            update_monitor_airtable(airtable_id,asset_status)                        
        if "Could not complete" not in input_fullpath:
            asset_status = "complete"
            update_monitor_airtable(airtable_id,asset_status)       
                               
