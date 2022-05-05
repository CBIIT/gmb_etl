#!/usr/bin/env python
# coding: utf-8

import requests
from requests.auth import HTTPBasicAuth
import yaml
from bs4 import BeautifulSoup
import pandas as pd
import os
import argparse
import boto3
import datetime
import dateutil.tz


######GET DATASET FROM RAVE######
print('GET DATASET FROM RAVE')
parser = argparse.ArgumentParser()
parser.add_argument('config_file')
args = parser.parse_args()
config = args.config_file
# auth = 'auth_gmb.yaml'
# config = 'gmb_config.yaml'
with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
r = requests.get(config['API'], auth = HTTPBasicAuth(config['USERNAME'], config['PASSWORD']))
data_set = r.content.decode("utf-8")
data = BeautifulSoup(data_set, features='lxml')

######TRANSFORM DATASET######
print('TRANSFORM DATASET')
data_dict = {}
for clinicaldata in data.odm:
    node_name = clinicaldata.subjectdata.studyeventdata.formdata['formoid']
    subject_key = clinicaldata.subjectdata['subjectkey']
    # add the subject key
    subject_key_name = 'SubjectKey'
    if node_name not in data_dict.keys():
        data_dict[node_name] = {}
    if node_name != 'SUBJECT':
        if subject_key_name not in data_dict[node_name].keys():
            data_dict[node_name][subject_key_name] = []
        data_dict[node_name][subject_key_name].append(subject_key)
    # add the type value
    type = 'type'
    if type not in data_dict[node_name].keys():
        data_dict[node_name][type] = []
    data_dict[node_name][type].append(node_name)
    for itemdata in clinicaldata.subjectdata.studyeventdata.formdata.itemgroupdata:
        itemoid = itemdata['itemoid'].split('.')
        if itemoid[1] not in data_dict[node_name].keys():
            data_dict[node_name][itemoid[1]] = []
        try:
            data_dict[node_name][itemoid[1]].append(itemdata['value'])
        except:
            data_dict[node_name][itemoid[1]].append(None)

######PRINT DATA FILES######
print('PRINT DATA FILES')
for node_type in data_dict:
    df = pd.DataFrame()
    for node_key in data_dict[node_type]:
        df[node_key] = data_dict[node_type][node_key]
    file_name = config['OUTPUT_FOLDER'] + node_type + ".tsv"
    if not os.path.exists(config['OUTPUT_FOLDER']):
        os.mkdir(config['OUTPUT_FOLDER'])
    df.to_csv(file_name, sep = "\t", index = False)


######VALIDATE DATA FILES######
print('VALIDATE DATA FILES')
with open(config['NODE_FILE']) as f:
    model = yaml.load(f, Loader = yaml.FullLoader)
for node in model['Nodes']:
    if node not in data_dict.keys():
        print(f'Data node {node} is not in the dataset.')
    else:
        for prop in model['Nodes'][node]['Props']:
            if prop not in data_dict[node].keys():
                print(f'Property {prop} from data node {node} is not in the dataset.')

######UPLOAD DATA FILES######

s3 = boto3.client('s3')
eastern = dateutil.tz.gettz('US/Eastern')
timestamp = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%dT%H%M%S")

for file_name in os.listdir(config['OUTPUT_FOLDER']):
    if file_name.endswith('.tsv'):
        file_directory = config['OUTPUT_FOLDER'] + file_name
        s3_file_directory = 'Raw' + '/' + timestamp + '/' + file_name
        s3.upload_file(file_directory ,config['S3_BUCKET'], s3_file_directory)
    
subfolder = config['S3_BUCKET'] + 'Raw' + '/' + timestamp
print(f'Data files upload to {subfolder}')


