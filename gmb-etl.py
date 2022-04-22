#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from requests.auth import HTTPBasicAuth
import yaml
from bs4 import BeautifulSoup
import pandas as pd
import os
import argparse

# In[2]:


######GET DATASET FROM RAVE######
print('GET DATASET FROM RAVE')
parser = argparse.ArgumentParser()
parser.add_argument('auth_file')
args = parser.parse_args()
auth = args.auth_file
# auth = 'auth_gmb.yaml'
with open(auth) as f:
    auth = yaml.load(f, Loader = yaml.FullLoader)
r = requests.get(auth['API'], auth = HTTPBasicAuth(auth['USERNAME'], auth['PASSWORD']))
data_set = r.content.decode("utf-8")
data = BeautifulSoup(data_set, features = 'lxml')


# In[3]:


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


# In[4]:


######PRINT DATA FILES######
print('PRINT DATA FILES')
for node_type in data_dict:
    df = pd.DataFrame()
    for node_key in data_dict[node_type]:
        df[node_key] = data_dict[node_type][node_key]
    file_name = auth['OUTPUT_FOLDER'] + node_type + ".tsv"
    if not os.path.exists(auth['OUTPUT_FOLDER']):
        os.mkdir(auth['OUTPUT_FOLDER'])
    df.to_csv(file_name, sep = "\t", index = False)


# In[5]:


######VALIDATE DATA FILES######
print('VALIDATE DATA FILES')
with open(auth['NODE_FILE']) as f:
    model = yaml.load(f, Loader = yaml.FullLoader)
for node in model['Nodes']:
    if node not in data_dict.keys():
        print(f'Data node {node} is not in the dataset.')

for node in model['Nodes']:
    if node in data_dict.keys():
        for prop in model['Nodes'][node]['Props']:
            if prop not in data_dict[node].keys():
                print(f'Property {prop} from data node {node} is not in the dataset.')


# In[ ]:




