# **GMB-ETL**
**Introduction and Usage Example**

GMB-ETL is a set of scripts to extract data from the Rave database and transforms the data to be ready to load into the Neo4j database.

To run the python scripts for extracting Gmb data from the Rave database and transforming Gmb data, the user can use the command below.

```python3 gmb-etl.py gmb_config_example.yaml```

To run the python script for extracting Gmb data from the Rave database only, the user can use the command below.

```python3 gmb-extraction.py gmb_config.yaml --extract-only```

To run the python script for transforming Gmb data, the user can use the command below.

```python3 gmb-transformation.py gmb_config_example.yaml s3_bucket_subfolder```

**Documentation Index**

This is the documentation index for the GMB-ETL.

- [GMB-ETL.md](docs/GMB-ETL.md)
- [GMB-ETL-Pipeline.md](docs/GMB-ETL-Pipeline.md)


