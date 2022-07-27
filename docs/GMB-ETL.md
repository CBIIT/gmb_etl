# **GMB-ETL**

This is the user documentation for the Gmb-ETL process.

**Introduction**

GMB-ETL is a set of scripts to extract data from the Rave database and transforms the data to be ready to load into the Neo4j database. After the data has been transformed, the next step is using the GMB-Dataloader and the GMB-OpenSearch-Loader to load data into the database. The GMB-ETL repo only has the extraction script and the transformation script, and the next step can be found on the Jenkins website.

**Pre-requisites**

- Python 3.6 or newer
- AWS S3 bucket
- Jenkins, if applicable

**Dependencies**

The user can run the command ````pip3 install -r requirements.txt```` to install dependencies.

- requests
- pyyaml
- bs4
- pandas
- argparse
- boto3
- datetime
- lxml

**Inputs**

- A GMB-ETL configuration file
- The S3 sub-folder name

**Outputs**

- The extraction script will upload the extracted data to the S3 bucket.
- The transformation script will first pull the data from the s3 bucket sub-folder and then upload the transformed data files to another s3 bucket sub-folder.

**Configuration File**

The configuration file contains all the variables that do not need to be changed through every GMB-ETL run. The fields of the configuration file are explained below. An example configuration file can be found in gmb\_config\_example.yaml.

- ````USERNAME````: The username to get access to the Rave database.
- ````PASSWORD````: The password to get access to the Rave database.
- ````API````: The API call to get data from the Rave database.
- ````OUTPUT_FOLDER````: The folder to store the data after running the extraction script.
- ````OUTPUT_NODE_FOLDER````: The folder to store the transformed data after running the transformation script.
- ````NODE_FILE````: The folder to store the node files from the GMB. These node files will help extract and transform the data.
- ````S3_BUCKET````: The name of the S3 bucket containing the data to be loaded.

**Command Line Arguments**

- Configuration File
  - The Yaml file contains the variables to run the extraction and the transformation scripts.
  - Command : ````<configuration file>````
  - Required for both the extraction and the transformation scripts.
- S3 bucket subfolder
  - The subfolder where the extraction data is loaded after running the extraction script.
  - Command: ````<s3 bucket subfolder>````
  - Required for the transformation script.

**Usage Example**

To run the python script for extracting Gmb data from the Rave database, the user can use the command<br/>
```python3 gmb-extraction.py <configuration file>```<br/>
To run the python script for transforming Gmb data, the user can use the command<br/>
```python3 gmb-transformation.py <configuration file> <s3 bucket subfolder>```<br/>