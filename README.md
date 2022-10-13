# gmb-etl extraction script
Code to extract data from Rave.<br/>
To run the python scripts for extracting the Gmb data from the Rave database and transforming Gmb data, the user can use the command<br/>
```python3 gmb-etl.py config/gmb_config_example.yaml```<br/>
To run the python script for extracting the Gmb data from the Rave database only, the user can use the command<br/>
```python3 gmb-extraction.py config/gmb_config.yaml --extract-only```<br/>
To run the python script for transforming the Gmb data which is downloaded from the s3 bucket, the user can use the command<br/>
```python3 gmb-transformation.py config/gmb_config_example.yaml s3_bucket_subfolder```<br/>
To run the python script for transforming the Gmb data which is in the local folder, the user can use the command<br/>
```python3 gmb-transformation.py config/gmb_config_example.yaml s3_bucket_subfolder --not_download_data```<br/>
