import os
import sys
import yaml
import argparse
import shutil
from gmb_transformation import GmbTransformation
from gmb_extraction import GmbExtraction
from bento.common.utils import get_logger

parser = argparse.ArgumentParser()
parser.add_argument('config_file', help='The name of the config file')
args = parser.parse_args()
config_file = args.config_file
with open(config_file) as f:
    config = yaml.load(f, Loader = yaml.FullLoader)
gmb_log = get_logger('GMB Main')

try:
    gmb_extract = GmbExtraction(config)
    timestamp = gmb_extract.extract()
except Exception as e:
    gmb_log.error(e)
    gmb_log.error('GMB data extraction fail, abort the GMB ETL process')
    sys.exit(1)

try:
    s3_sub_folder = timestamp
    download_data = False
    gmb_trans = GmbTransformation(config_file, s3_sub_folder, download_data)
    gmb_trans.transform()
except Exception as e:
    gmb_log.error(e)
    gmb_log.error('GMB data transformation fail, abort the GMB ETL process')
    sys.exit(1)

try:
    for static_file in os.listdir(config['STATIC_FILES']):
        shutil.copy(os.path.join(config['STATIC_FILES'], static_file) , config['OUTPUT_NODE_FOLDER'])
except Exception as e:
    gmb_log.error(e)
    gmb_log.error('GMB static files copying fail, abort the GMB ETL process')
    sys.exit(1)

data_loader_command = 'python3 ' + config['DATA_LOADER'] + ' ' + config['DATA_LOADER_CONFIG'] + ' --no-backup --dataset ' + config['OUTPUT_NODE_FOLDER']
os.system(data_loader_command)






