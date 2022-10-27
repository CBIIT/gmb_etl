import os
import sys
import yaml
import argparse
import shutil
from gmb_transformation import GmbTransformation
from gmb_extraction import GmbExtraction
from bento.common.utils import get_logger
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('config_file', help='The name of the config file')
args = parser.parse_args()
config_file = args.config_file
with open(config_file) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
gmb_log = get_logger('GMB Main')

try:
    # Extract data files
    gmb_extraction = GmbExtraction(config)
    timestamp = gmb_extraction.extract()
except Exception as e:
    gmb_log.error(e)
    gmb_log.error('GMB data extraction failed, abort the GMB ETL process')
    sys.exit(1)


try:
    # Transform data files
    s3_sub_folder = timestamp
    download_data = False
    gmb_trans = GmbTransformation(config_file, s3_sub_folder, download_data)
    gmb_trans.transform()
except Exception as e:
    gmb_log.error(e)
    gmb_log.error('GMB data transformation failed, abort the GMB ETL process')
    sys.exit(1)

try:
    # Copy static files to the transformed data files' folder
    for static_file in os.listdir(config['STATIC_FILES']):
        shutil.copy(os.path.join(config['STATIC_FILES'], static_file) , config['OUTPUT_FOLDER_TRANSFORMED'])
except Exception as e:
    gmb_log.error(e)
    gmb_log.error('GMB static files copying failed, abort the GMB ETL process')
    sys.exit(1)

# Load data files to the neo4j database
data_loader_command = ['python3', config['DATA_LOADER'], config['DATA_LOADER_CONFIG'], '--dataset', config['OUTPUT_FOLDER_TRANSFORMED']]
data_loader_result = subprocess.call(data_loader_command)
if data_loader_result != 0:
    # if something is wrong while running the data loader
    gmb_log.error('GMB data upload failed, abort the GMB ETL process')
    sys.exit(1)






