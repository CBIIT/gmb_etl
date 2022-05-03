#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import yaml
import random
import boto3
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('config_file')
parser.add_argument('s3_sub_folder')
args = parser.parse_args()
config = args.config_file
#config = 'gmb_config.yaml'


#Download file from s3
def download_from_s3(s3, args_s3_sub_folder):
    subfolder = 'Raw/' + args_s3_sub_folder
    subfolder_dirsctory = './' + args_s3_sub_folder + '/'
    for key in s3.list_objects(Bucket = config['S3_BUCKET'])['Contents']:
        file_name = key['Key'].split('/')
        file_key = subfolder_dirsctory + file_name[2]

        if subfolder in key['Key']:
            if not os.path.exists(subfolder_dirsctory):
                os.mkdir(subfolder_dirsctory)
            s3.download_file(config['S3_BUCKET'], key['Key'], file_key)
#Upload files to s3
def upload_files(s3, config, timestamp):
    for file_name in os.listdir(config['OUTPUT_NODE_FOLDER']):
        if file_name.endswith('.tsv'):
            file_directory = config['OUTPUT_NODE_FOLDER'] + file_name
            s3_file_directory = 'Transformed' + '/' + timestamp + '/' + file_name
            s3.upload_file(file_directory ,config['S3_BUCKET'], s3_file_directory)
#Add id field for data file
def add_id_field(parent_node, df, file_name):
    if file_name[0] != parent_node:
        node_id_number_list = random.sample(range(10**5, 10**6), len(df))
        for x in range(0, len(df)):
            node_id_number_list[x] = file_name[0] + '-' + str(node_id_number_list[x])
        id_key = file_name[0] + '_id'
        df[id_key] = node_id_number_list
    return df
#Rename node
def rename_node(old_node_name, new_node_name, df, file_name):
    if file_name[0] == old_node_name:
        file_name[0] = new_node_name
        type_list = [new_node_name] * len(df)
        df['type'] = type_list
    return df, file_name
#Rename properties
def rename_properties(old_property, new_property, df):
    for x in range(0, len(old_property)):
        df = df.rename(columns={old_property[x]: new_property[x]})
    return df
#Remove properties that are not in the data_node file
def remove_properties(df, node, node_name):
    remove_list = []
    property_list = df.columns.tolist()
    for property in property_list:
        if property not in node['Props'] and property != 'type' and '.' not in property:
            remove_list.append(property)
    if len(remove_list) == 0:
        print(f'Data node {node_name} does not have any properties to remove')
    else:
        df = df.drop(columns = remove_list)
        print(f'Data node {node_name} removes {remove_list}')
    return df
            


with open(config) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
s3 = boto3.client('s3')
download_from_s3(s3, args.s3_sub_folder)
download_file_directory = './' + args.s3_sub_folder
print('Files are successfully downloaded')

with open(config['NODE_FILE']) as f:
    model = yaml.load(f, Loader = yaml.FullLoader)    
for file_name in os.listdir(download_file_directory):
    if file_name.endswith(".tsv"):
        df = pd.read_csv(os.path.join(download_file_directory, file_name), sep='\t')
        file_name = file_name.split('.')
        #Rename node
        df, file_name = rename_node('PHYSICAL_EXAM___SCREENING', 'PHYSICAL_EXAM_SCREENING', df, file_name)
        if file_name[0] in model['Nodes'].keys():
            df = add_id_field('SUBJECT', df, file_name)
            #Rename SubjectKey
            old_property = ['SubjectKey', 'REG_INST_ID_CD_ENR', 'GRMLN_VAR_PTHGNC_CAT']
            new_property = ['SUBJECT.PT_ID','REG_INST_ID_CD','SOMATIC_VAR_PTHGNC_CAT']
            df = rename_properties(old_property, new_property, df)
            #Remove property
            df = remove_properties(df, model['Nodes'][file_name[0]], file_name[0])
            #Print the data
            file_name = config['OUTPUT_NODE_FOLDER'] + file_name[0] + '.tsv'
            if not os.path.exists(config['OUTPUT_NODE_FOLDER']):
                os.mkdir(config['OUTPUT_NODE_FOLDER'])
            df.to_csv(file_name, sep = "\t", index = False)
        else:
            print(f'{file_name[0]} is not in the node file')


######UPLOAD DATA FILES######
upload_files(s3, config, args.s3_sub_folder)
print('Files are successfully uploaded')
