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
from gmb_transformation import gmb_transformation

class gmb_extraction():
    def __init__(self, config_file):
    ######GET DATASET FROM RAVE######
        self.log = get_logger('GMB Transformation')
        self.log.info('GET DATASET FROM RAVE')
        config = config_file
        with open(config) as f:
            self.config = yaml.load(f, Loader = yaml.FullLoader)
        r = requests.get(self.config['API'], auth = HTTPBasicAuth(self.config['USERNAME'], self.config['PASSWORD']))
        data_set = r.content.decode("utf-8")
        self.data = BeautifulSoup(data_set, features='lxml')


    ######TRANSFORM DATASET######
    def transform_data(self):
        self.log.info('TRANSFORM DATASET')
        data_dict = {}
        for clinicaldata in self.data.odm:
            if clinicaldata['metadataversionoid'] == str(self.config['VERSION_NUMBER']):
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
        return data_dict

        ######PRINT DATA FILES######
    def print_data(self, data_dict):
        self.log.info('PRINT DATA FILES')
        for node_type in data_dict:
            df = pd.DataFrame()
            for node_key in data_dict[node_type]:
                df[node_key] = data_dict[node_type][node_key]
            file_name = self.config['OUTPUT_FOLDER'] + node_type + ".tsv"
            if not os.path.exists(self.config['OUTPUT_FOLDER']):
                os.mkdir(self.config['OUTPUT_FOLDER'])
            df.to_csv(file_name, sep = "\t", index = False)

    def validate_files(self, data_dict):
        ######VALIDATE DATA FILES######
        self.log.info('VALIDATE DATA FILES')
        if len(data_dict) == 0:
            self.log.error('The extraction script did not extract any data, abort uploading data to s3.')
            sys.exit()
        with open(self.config['NODE_FILE']) as f:
            model = yaml.load(f, Loader = yaml.FullLoader)
        for node in model['Nodes']:
            if node not in data_dict.keys():
                self.log.warning(f'Data node {node} is not in the dataset.')
            else:
                for prop in model['Nodes'][node]['Props']:
                    if prop not in data_dict[node].keys():
                        self.log.warning(f'Property {prop} from data node {node} is not in the dataset.')

        ######UPLOAD DATA FILES######
    def upload_files(self):
        s3 = boto3.client('s3')
        eastern = dateutil.tz.gettz('US/Eastern')
        timestamp = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%dT%H%M%S")

        for file_name in os.listdir(self.config['OUTPUT_FOLDER']):
            if file_name.endswith('.tsv'):
                file_directory = self.config['OUTPUT_FOLDER'] + file_name
                s3_file_directory = 'Raw' + '/' + timestamp + '/' + file_name
                s3.upload_file(file_directory, self.config['S3_BUCKET'], s3_file_directory)

        subfolder = 's3://' + self.config['S3_BUCKET'] + '/' + 'Raw' + '/' + timestamp
        self.log.info(f'Data files upload to {subfolder}')
        return timestamp

    def extract(self):
        data_dict = self.transform_data()
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
    gmb_extract = gmb_extraction(config_file)
    timestamp = gmb_extract.extract()

    if args.extract_only != True:
        config = args.config_file
        s3_sub_folder = timestamp
        download_data = True
        gmb_trans = gmb_transformation(config, s3_sub_folder, download_data)
        gmb_trans.transform()
