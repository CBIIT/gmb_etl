# gmb-etl extraction script
Code to extract data from Rave.<br/>
Run ```pip3 install -r requirements.txt``` to install dependencies<br/>
To run the python scripts for extracting the Gmb data from the Rave database and transforming Gmb data, the user can use the command<br/>
```python3 gmb_extraction.py config/gmb_config_example.yaml```<br/>
To run the python script for extracting the Gmb data from the Rave database only, the user can use the command<br/>
```python3 gmb_extraction.py config/gmb_config.yaml --extract-only```<br/>
To run the python script for transforming the Gmb data which is downloaded from the s3 bucket, the user can use the command<br/>
```python3 gmb_transformation.py config/gmb_config_example.yaml s3_bucket_subfolder```<br/>
To run the python script for transforming the Gmb data which is in the local folder, the user can use the command<br/>
```python3 gmb_transformation.py config/gmb_config_example.yaml s3_bucket_subfolder --not_download_data```<br/>
After running the code, the user can check the log for more validation related information, and the user also needs to add 3 more files(CLINICALTRIAL.tsv, SITE.tsv, internal-whitelist.tsv) before uploading data through the data-loader.