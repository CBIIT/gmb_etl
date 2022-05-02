#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import yaml
import random
import boto3
import datetime
import dateutil.tz
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('config_file')
parser.add_argument('s3_sub_folder')
args = parser.parse_args()
config = args.config_file
#config = 'gmb_config.yaml'


#download file from s3
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
#upload files to s3
def upload_files(s3, config, timestamp):
    for file_name in os.listdir(config['OUTPUT_NODE_FOLDER']):
        if file_name.endswith('.tsv'):
            file_directory = config['OUTPUT_NODE_FOLDER'] + file_name
            s3_file_directory = 'Transformed' + '/' + timestamp + '/' + file_name
            s3.upload_file(file_directory ,config['S3_BUCKET'], s3_file_directory)
#add id field for data file
def add_id_field(parent_node, df, file_name):
    if file_name[0] != parent_node:
        node_id_number_list = random.sample(range(10**5, 10**6), len(df))
        for x in range(0, len(df)):
            node_id_number_list[x] = file_name[0] + '-' + str(node_id_number_list[x])
        id_key = file_name[0] + '_id'
        df[id_key] = node_id_number_list
    return df
#transform node name
def transform_node_name(old_node_name, new_node_name, df, file_name):
    if file_name[0] == old_node_name:
        file_name[0] = new_node_name
        type_list = [new_node_name] * len(df)
        df['type'] = type_list
    return df, file_name

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
        df, file_name = transform_node_name('PHYSICAL_EXAM___SCREENING', 'PHYSICAL_EXAM_SCREENING', df, file_name)
        if file_name[0] in model['Nodes'].keys():
            df = add_id_field('SUBJECT', df, file_name)
            # rename SubjectKey
            df = df.rename(columns={'SubjectKey': 'SUBJECT.PT_ID'})
            # Print the data
            file_name = config['OUTPUT_NODE_FOLDER'] + file_name[0] + '.tsv'
            if not os.path.exists(config['OUTPUT_NODE_FOLDER']):
                os.mkdir(config['OUTPUT_NODE_FOLDER'])
            df.to_csv(file_name, sep = "\t", index = False)
        else:
            print(f'{file_name[0]} is not in the node file')


######UPLOAD DATA FILES######
upload_files(s3, config, args.s3_sub_folder)
print('Files are successfully uploaded')
