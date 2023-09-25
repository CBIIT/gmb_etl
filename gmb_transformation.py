#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import yaml
import random
import boto3
import argparse
from bento.common.utils import get_logger

class GmbTransformation():
    def __init__(self, config_file, s3_sub_folder, download_data):
        self.s3_sub_folder = s3_sub_folder
        self.download_data = download_data
        self.log = get_logger('GMB Transformation')
        config = config_file
        with open(config) as f:
            self.config = yaml.load(f, Loader = yaml.FullLoader)

    def download_from_s3(self, s3):
        # Function to download raw data files from the s3 bucket
        # The user can decide use this function to get raw data or just read raw data from local
        # 's3' is a boto3 s3 object
        subfolder = 'Raw/' + self.s3_sub_folder
        subfolder_dirsctory = './' + self.s3_sub_folder + '/'
        for key in s3.list_objects(Bucket = self.config['S3_BUCKET'], Prefix = subfolder)['Contents']:
            file_name = key['Key'].split('/')
            file_key = subfolder_dirsctory + file_name[2]
            if not os.path.exists(subfolder_dirsctory):
                # If the path does not exist, then create the folder
                os.mkdir(subfolder_dirsctory)
            s3.download_file(self.config['S3_BUCKET'], key['Key'], file_key)

    def upload_files(self, s3):
        # Function to upload transformed data files to the s3 bucket
        # Transformed data will have the same sub-folder name as the raw data
        # 's3' is a boto3 s3 object
        timestamp = self.s3_sub_folder
        for file_name in os.listdir(self.config['OUTPUT_FOLDER_TRANSFORMED']):
            if file_name.endswith('.tsv'):
                # Find every file that end with '.tsv' and upload them to se bucket
                file_directory = self.config['OUTPUT_FOLDER_TRANSFORMED'] + file_name
                s3_file_directory = 'Transformed' + '/' + timestamp + '/' + file_name
                s3.upload_file(file_directory ,self.config['S3_BUCKET'], s3_file_directory)
        for file_name in os.listdir(self.config['STATIC_FILES']):
            if file_name.endswith('.tsv'):
                file_directory = self.config['STATIC_FILES'] + file_name
                s3_file_directory = 'Transformed' + '/' + timestamp + '/' + file_name
                s3.upload_file(file_directory ,self.config['S3_BUCKET'], s3_file_directory)
        subfolder = 's3://' + self.config['S3_BUCKET'] + '/' + 'Transformed' + '/' + timestamp
        self.log.info(f'Data files upload to {subfolder}')

    def add_id_field(self, df, file_name):
        # Function to add id field for each data frame
        # The function will not id field to the data file 'SUBJECT' since it have it's id field
        # 'df' is the data frame of the data node
        # 'file_name' is the name of the data node
        parent_node = 'SUBJECT'
        if file_name[0] != parent_node:
            # If the name of data node is not 'SUBJECT', then it is not a parent node and need to be added id field
            node_id_number_list = random.sample(range(10**5, 10**6), len(df))
            for x in range(0, len(df)):
                node_id_number_list[x] = file_name[0] + '-' + str(node_id_number_list[x])
            id_key = file_name[0] + '_id'
            df[id_key] = node_id_number_list
        return df

    def rename_properties(self, df):
        # Function to rename some properties inside the data frame
        # The user can add new properties that need to be renamed into the 'proeprty' list
        property = [
            {'old':'SubjectKey', 'new':'SUBJECT.PT_ID'},
            {'old':'REG_INST_ID_CD_ENR', 'new':'REG_INST_ID_CD'},
            {'old':'GRMLN_VAR_PTHGNC_CAT', 'new':'SOMATIC_VAR_PTHGNC_CAT'}
        ]
        for x in range(0, len(property)):
            df = df.rename(columns={property[x]['old']: property[x]['new']})
        return df

    def remove_properties(self, df, node, node_name):
        # Function to remove proerties that are not in the model file
        # The removed properties will be print out in logs
        # 'node' is the model file
        # 'df' is the data frame of the data node
        # 'node_name' is the name of the data node
        remove_list = []
        property_list = df.columns.tolist()
        for property in property_list:
            if property not in node['Props'] and property != 'type' and '.' not in property and property != node_name + '_id':
                # If the property is not in the model file, it is not 'type', and it is not the id_field, then the property will be added to the remove_list
                remove_list.append(property)
        if len(remove_list) == 0:
            # If there is nothing to remove
            self.log.info(f'Data node {node_name} does not have any properties to remove')
        else:
            # If the remove_list length is not 0, then remove all the proeerties in the remove list
            df = df.drop(columns = remove_list)
            self.log.info(f'Data node {node_name} removes {remove_list}')
        return df

    def rename_node(self, df, file_name):
        # Function to rename the raw data files.
        # The user can add the raw data files that need to be renamed in the 'rename_nodes' list
        # 'df' is the data frame of the data node
        # 'file_name' is the name of the data node
        rename_nodes = [
            {'old':'PHYSICAL_EXAM___SCREENING', 'new':'PHYSICAL_EXAM_SCREENING'}
            ]
        for node in rename_nodes:
            if file_name[0] == node['old']:
                # If the data node is the one in the list, then the function will rename the data ndoe's name
                file_name[0] = node['new']
                type_list = [node['new']] * len(df)
                df['type'] = type_list

        return df, file_name

    def copy_properties(self, file_name, df):
        # Function to create a new property through copying the pre-exsisting property
        # The user can add the properties that need to be copied to the 'props' list
        # 'df' is the data frame of the data node
        # 'file_name' is the name of the data node
        props = [
            {'node':'SUBJECT', 'new_property':'SITE.REG_INST_ID', 'copy_property':'REG_INST_ID_CD_NM'}
        ]
        for property in props:
            if property['node'] == file_name[0]:
                # If the data node is the one in the list, then the function will copy the proerty
                df[property['new_property']] = df[property['copy_property']]

        return df

    def add_properties(self, file_name, df):
        # Function to add properties to the data frames
        # The user can add the properties and the values for the properties that need to be added to the 'props' list
        # 'df' is the data frame of the data node
        # 'file_name' is the name of the data node
        props = [
            {'node':'SUBJECT', 'new_property':'CLINICALTRIAL.CLINICAL_TRIAL_ID', 'new_value':['NCT04706663'] * len(df)}
        ]
        for property in props:
            if property['node'] == file_name[0]:
                # If the data node is the one in the list, then the function will add the property to the data frame of the data node from the list
                df[property['new_property']] = property['new_value']

        return df

    def print_data(self, file_name, df):
        # Function to store the transformed data frames to local csv files
        # 'df' is the data frame of the data node
        # 'file_name' is the name of the data node
        file_name = self.config['OUTPUT_FOLDER_TRANSFORMED'] + file_name[0] + '.tsv'
        if not os.path.exists(self.config['OUTPUT_FOLDER_TRANSFORMED']):
            # If the path does not exist, then create the folder
            os.mkdir(self.config['OUTPUT_FOLDER_TRANSFORMED'])
        df.to_csv(file_name, sep = "\t", index = False)


    def transform(self):
        # Function to transform the raw data using the fuctions created previously
        s3 = boto3.client('s3')
        if self.download_data:
            # If 'self.download_data' is true, then download data from s3, otherwise read raw data from local
            self.download_from_s3(s3)
            download_file_directory = os.path.join('./', self.s3_sub_folder)
            self.log.info('Files are successfully downloaded')
        else:
            download_file_directory = self.config['OUTPUT_FOLDER_RAW']
            self.log.info('Transforming local data at {}'.format(self.config['OUTPUT_FOLDER_RAW']))

        with open(self.config['DATA_MODEL_NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        for file_name in os.listdir(download_file_directory):
            if file_name.endswith(".tsv"):
                # Get all the raw data files from the folder
                df = pd.read_csv(os.path.join(download_file_directory, file_name), sep='\t')
                file_name = file_name.split('.')
                df, file_name = self.rename_node(df, file_name)
                if file_name[0] in model['Nodes'].keys():
                    # If the file_name is in the model files, then do the transformation
                    df = self.add_id_field(df, file_name)
                    df = self.rename_properties(df)
                    df = self.copy_properties(file_name, df)
                    df = self.add_properties(file_name, df)
                    df = self.remove_properties(df, model['Nodes'][file_name[0]], file_name[0])
                    self.print_data(file_name, df)
                else:
                    self.log.info(f'{file_name[0]} is not in the node file')

        #download the static files before upload
        for key in s3.list_objects(Bucket = self.config['S3_BUCKET'], Prefix = self.config['STATIC_FILES'])['Contents']:
            if key['Key'].endswith(".tsv"):
                if not os.path.exists(self.config['STATIC_FILES']):
                # If the path does not exist, then create the folder
                    os.mkdir(self.config['STATIC_FILES'])
                static_file_name = key['Key'].split('/')
                static_file_key = self.config['STATIC_FILES'] + static_file_name[1]
                s3.download_file(self.config['S3_BUCKET'], key['Key'], static_file_key)
        

        self.upload_files(s3)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    parser.add_argument('s3_sub_folder')
    parser.add_argument('--local-raw-data', help='Decide whether or not download data from s3 for transformation', action='store_true')
    args = parser.parse_args()
    config = args.config_file
    s3_sub_folder = args.s3_sub_folder
    download_data = not args.local_raw_data
    gmb_transformer = GmbTransformation(config, s3_sub_folder, download_data)
    gmb_transformer.transform()