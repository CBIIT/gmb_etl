#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import yaml
import random


# In[5]:


config = 'gmb_config.yaml'
with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
with open(config['NODE_FILE']) as f:
    model = yaml.load(f, Loader = yaml.FullLoader)    
for file_name in os.listdir(config['OUTPUT_FOLDER']):
    if file_name.endswith(".tsv"):
        df = pd.read_csv(os.path.join(config['OUTPUT_FOLDER'], file_name), sep='\t')
        file_name = file_name.split('.')
        if file_name[0] == 'PHYSICAL_EXAM___SCREENING':
            file_name[0] = 'PHYSICAL_EXAM_SCREENING'
            type_list = ['PHYSICAL_EXAM_SCREENING'] * len(df)
            df['type'] = type_list
        if file_name[0] in model['Nodes'].keys():
            #for prop in model['Nodes'][file_name[0]]['Props']:
                #if prop not in df.keys():
                    # add empty string
                    #df[prop] = ''
            if file_name[0] != 'SUBJECT':
                node_id_number_list = random.sample(range(10**5, 10**6), len(df))
                for x in range(0, len(df)):
                    node_id_number_list[x] = file_name[0] + '-' + str(node_id_number_list[x])
                id_key = file_name[0] + '_id'
                df[id_key] = node_id_number_list
            # rename SubjectKey
            df = df.rename(columns={'SubjectKey': 'SUBJECT.PT_ID'})
            # print(f'Node {file_name[0]} can not find SubjectKey')
            # Print the data
            file_name = config['OUTPUT_NODE_FOLDER'] + file_name[0] + '.tsv'
            if not os.path.exists(config['OUTPUT_NODE_FOLDER']):
                os.mkdir(config['OUTPUT_NODE_FOLDER'])
            df.to_csv(file_name, sep = "\t", index = False)
        else:
            print(f'{file_name[0]} is not in the node file')


# In[ ]:




