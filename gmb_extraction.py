#!/usr/bin/env python
# coding: utf-8

from time import time
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
import sys
from bento.common.utils import get_logger
from gmb_transformation import GmbTransformation

class GmbExtraction():
    def __init__(self, config):
        self.log = get_logger('GMB Extraction')
        self.log.info('GET DATASET FROM RAVE')
        self.config = config

    def cleanup_data(self, data):
        # Function to clean the data that was gotten from RAVE
        # Function to populate the data dict with the data that was from RAVE
        self.log.info('CLEAN UP DATASET')
        data_dict = {}
        for clinicaldata in data.odm:
            if clinicaldata['metadataversionoid'] == str(self.config['RAVE_DATA_VERSION']):
                node_name = clinicaldata.subjectdata.studyeventdata.formdata['formoid']
                subject_key = clinicaldata.subjectdata['subjectkey'] # add the subject key
                subject_key_name = 'SubjectKey'
                if node_name not in data_dict.keys():
                    data_dict[node_name] = {}
                if node_name != 'SUBJECT':
                    if subject_key_name not in data_dict[node_name].keys():
                        data_dict[node_name][subject_key_name] = []
                    data_dict[node_name][subject_key_name].append(subject_key)
                type = 'type' # add the type value
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
        return data_dict

    def print_data(self, data_dict):
        # Function to store the raw data frames to local csv files
        self.log.info('PRINT DATA FILES')
        for node_type in data_dict:
            df = pd.DataFrame()
            for node_key in data_dict[node_type]:
                df[node_key] = data_dict[node_type][node_key]
            file_name = self.config['OUTPUT_FOLDER_RAW'] + node_type + ".tsv"
            if not os.path.exists(self.config['OUTPUT_FOLDER_RAW']):
                os.mkdir(self.config['OUTPUT_FOLDER_RAW'])
            df.to_csv(file_name, sep = "\t", index = False)

    def validate_files(self, data_dict):
        # Function to validate the data that was pulled from RAVE
        # Function will warn the user if the raw data files or the property of the data is not in the model file
        self.log.info('VALIDATE DATA FILES')
        if len(data_dict) == 0:
            self.log.error('The extraction script did not extract any data, abort uploading data to s3.')
            sys.exit()
        with open(self.config['DATA_MODEL_NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        for node in model['Nodes']:
            if node not in data_dict.keys():
                self.log.warning(f'Data node {node} is not in the dataset.')
            else:
                for prop in model['Nodes'][node]['Props']:
                    if prop not in data_dict[node].keys():
                        self.log.warning(f'Property {prop} from data node {node} is not in the dataset.')

    def upload_files(self):
        # Function to upload the raw data to the s3 bucket
        # The subfolder name of the uploaded data will be timestamp
        s3 = boto3.client('s3')
        eastern = dateutil.tz.gettz('US/Eastern')
        timestamp = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%dT%H%M%S")

        for file_name in os.listdir(self.config['OUTPUT_FOLDER_RAW']):
            if file_name.endswith('.tsv'):
                file_directory = self.config['OUTPUT_FOLDER_RAW'] + file_name
                s3_file_directory = 'Raw' + '/' + timestamp + '/' + file_name
                s3.upload_file(file_directory, self.config['S3_BUCKET'], s3_file_directory)

        subfolder = 's3://' + self.config['S3_BUCKET'] + '/' + 'Raw' + '/' + timestamp
        self.log.info(f'Data files upload to {subfolder}')
        return timestamp

    def extract(self):
        # Function to extract data
        r = requests.get(self.config['API'], auth = HTTPBasicAuth(self.config['USERNAME'], self.config['PASSWORD'])) # Download data from RAVE
        data_set = r.content.decode("utf-8")
        data = BeautifulSoup(data_set, features='lxml')
        data_dict = self.cleanup_data(data)
        self.print_data(data_dict)
        self.validate_files(data_dict)
        timestamp = self.upload_files()
        return timestamp


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help='The name of the config file')
    parser.add_argument('--extract-only', help='Decide whether or not run only the extraction script', action='store_true')
    args = parser.parse_args()
    config_file = args.config_file
    with open(config_file) as f:
        config = yaml.load(f, Loader = yaml.FullLoader)
    gmb_extraction = GmbExtraction(config)
    timestamp = gmb_extraction.extract()

    if args.extract_only != True:
        # if not only extract the data but also transform the data
        s3_sub_folder = timestamp
        download_data = False
        gmb_transformation = GmbTransformation(config_file, s3_sub_folder, download_data)
        gmb_transformation.transform()
