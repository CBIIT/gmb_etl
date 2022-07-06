# gmb-etl extraction script
Code to extract data from Rave.<br/>
To run the python scripts for extracting Gmb data from the Rave database and transforming Gmb data, the user can use the command<br/>
```python3 gmb-etl.py gmb_config_example.yaml```<br/>
To run the python script for extracting Gmb data from the Rave database only, the user can use the command<br/>
```python3 gmb-extraction.py gmb_config.yaml --extract-only```<br/>
To run the python script for transforming Gmb data, the user can use the command<br/>
```python3 gmb-transformation.py gmb_config_example.yaml s3_bucket_subfolder```<br/>
