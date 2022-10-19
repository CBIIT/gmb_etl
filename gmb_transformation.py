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
    #Download file from s3
    def download_from_s3(self, s3):
        subfolder = 'Raw/' + self.s3_sub_folder
        subfolder_dirsctory = './' + self.s3_sub_folder + '/'
        for key in s3.list_objects(Bucket = self.config['S3_BUCKET'], Prefix = subfolder)['Contents']:
            file_name = key['Key'].split('/')
            file_key = subfolder_dirsctory + file_name[2]
            if not os.path.exists(subfolder_dirsctory):
                os.mkdir(subfolder_dirsctory)
            s3.download_file(self.config['S3_BUCKET'], key['Key'], file_key)
    #Upload files to s3
    def upload_files(self, s3):
        timestamp = self.s3_sub_folder
        for file_name in os.listdir(self.config['OUTPUT_NODE_FOLDER']):
            if file_name.endswith('.tsv'):
                file_directory = self.config['OUTPUT_NODE_FOLDER'] + file_name
                s3_file_directory = 'Transformed' + '/' + timestamp + '/' + file_name
                s3.upload_file(file_directory ,self.config['S3_BUCKET'], s3_file_directory)
        subfolder = 's3://' + self.config['S3_BUCKET'] + '/' + 'Transformed' + '/' + timestamp
        self.log.info(f'Data files upload to {subfolder}')
    #Add id field for data file
    def add_id_field(self, df, file_name):
        parent_node = 'SUBJECT'
        if file_name[0] != parent_node:
            node_id_number_list = random.sample(range(10**5, 10**6), len(df))
            for x in range(0, len(df)):
                node_id_number_list[x] = file_name[0] + '-' + str(node_id_number_list[x])
            id_key = file_name[0] + '_id'
            df[id_key] = node_id_number_list
        return df
    #Rename properties
    def rename_properties(self, df):
        property = [
            {'old':'SubjectKey', 'new':'SUBJECT.PT_ID'},
            {'old':'REG_INST_ID_CD_ENR', 'new':'REG_INST_ID_CD'},
            {'old':'GRMLN_VAR_PTHGNC_CAT', 'new':'SOMATIC_VAR_PTHGNC_CAT'}
        ]
        for x in range(0, len(property)):
            df = df.rename(columns={property[x]['old']: property[x]['new']})
        return df
    #Remove properties that are not in the data_node file
    def remove_properties(self, df, node, node_name):
        remove_list = []
        property_list = df.columns.tolist()
        for property in property_list:
            if property not in node['Props'] and property != 'type' and '.' not in property and property != node_name + '_id':
                remove_list.append(property)
        if len(remove_list) == 0:
            self.log.info(f'Data node {node_name} does not have any properties to remove')
        else:
            df = df.drop(columns = remove_list)
            self.log.info(f'Data node {node_name} removes {remove_list}')
        return df
    #Rename node
    def rename_node(self, df, file_name):
        # 'PHYSICAL_EXAM___SCREENING', 'PHYSICAL_EXAM_SCREENING',
        rename_nodes = [
            {'old':'PHYSICAL_EXAM___SCREENING', 'new':'PHYSICAL_EXAM_SCREENING'}
            ]
        for node in rename_nodes:
            if file_name[0] == node['old']:
                file_name[0] = node['new']
                type_list = [node['new']] * len(df)
                df['type'] = type_list

        return df, file_name

    #Copy property
    def copy_properties(self, file_name, df):
        props = [
            {'node':'SUBJECT', 'new_property':'SITE.REG_INST_ID', 'copy_property':'REG_INST_ID_CD_NM'}
        ]
        for property in props:
            if property['node'] == file_name[0]:
                df[property['new_property']] = df[property['copy_property']]

        return df

    #Add property
    def add_properties(self, file_name, df):
        props = [
            {'node':'SUBJECT', 'new_property':'CLINICALTRIAL.CLINICAL_TRIAL_ID', 'new_value':['NCT04706663'] * len(df)}
        ]
        for property in props:
            if property['node'] == file_name[0]:
                df[property['new_property']] = property['new_value']

        return df

    #self.log.info Data
    def print_data(self, file_name, df):
        file_name = self.config['OUTPUT_NODE_FOLDER'] + file_name[0] + '.tsv'
        if not os.path.exists(self.config['OUTPUT_NODE_FOLDER']):
            os.mkdir(self.config['OUTPUT_NODE_FOLDER'])
        df.to_csv(file_name, sep = "\t", index = False)


    def transform(self):
        s3 = boto3.client('s3')
        if self.download_data:
            self.download_from_s3(s3)
            download_file_directory = os.path.join('./', self.s3_sub_folder)
            self.log.info('Files are successfully downloaded')
        else:
            download_file_directory = self.config['OUTPUT_FOLDER']
            self.log.info('Transforming local data at {}'.format(self.config['OUTPUT_FOLDER']))

        with open(self.config['NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        for file_name in os.listdir(download_file_directory):
            if file_name.endswith(".tsv"):
                df = pd.read_csv(os.path.join(download_file_directory, file_name), sep='\t')
                file_name = file_name.split('.')
                #Rename node
                df, file_name = self.rename_node(df, file_name)
                if file_name[0] in model['Nodes'].keys():
                    df = self.add_id_field(df, file_name)
                    #Rename property
                    df = self.rename_properties(df)
                    #Copy property
                    df = self.copy_properties(file_name, df)
                    #Add property
                    df = self.add_properties(file_name, df)
                    #Remove property
                    df = self.remove_properties(df, model['Nodes'][file_name[0]], file_name[0])
                    #self.log.info the data
                    self.print_data(file_name, df)
                else:
                    self.log.info(f'{file_name[0]} is not in the node file')


        ######UPLOAD DATA FILES######
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
    gmb_trans = GmbTransformation(config, s3_sub_folder, download_data)
    gmb_trans.transform()