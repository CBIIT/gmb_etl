**GMB-ETL PIPELINE**

This is the user documentation for the Gmb-ETL pipeline in Jenkins.

**Introduction**

GMB-ETL pipeline in Jenkins will help the user extract and transform the
extracted data into the data-loading ready form by selecting the choice
instead of running the script from and command prompt. Once the pipeline
is finished, the user needs to download the data file from the s3 bucket
and upload it to another S3 bucket waiting for the uploading of the data
to the GMB website.

**Usage Example**

-  After logging into Jenkins, the user will find the Gmb-ETL pipeline
    under the Gmb directory in Jenkins.

![Graphical user interface, application Description automatically
generated](media/image1.png)


-  Once the user is inside the ETL pipeline, the user can choose the
    "Build with Parameters" section.

-  Inside the "Build with Parameters", the user will see serval
    sections. The "RaveEnvironment" will decide which database the user
    wants their data extracted.

-  The users can choose what operations they want to do in the
    "ETLOperation" section. The users can choose between "Extract",
    "ExtractAndTransform", and "Transform". The "Extract" option will
    only do the data extraction, the "Transform" option will only do the
    data transformation, and the "ExtractAndTransform" option will do
    both the data extraction and the data transformation at the same
    time.

-  The user can choose which bucket they want to store or read data
    from in the section "S3Bucket".

-  The "S3BucketSubFolder" will be needed if the users choose the
    "Transform" option in the section "ETLOperation". The subfolder
    value can be found in the console output messages after executing
    the "Extract" option in the section "ETLOperation".

![Graphical user interface, application Description automatically
generated](media/image2.png)

-  After getting the transformed data using the GMB-ETL pipeline, the
    user needs to download the transformed data from the S3 bucket
    subfolder, and the subfolder value can be obtained by going through
    the console output messages after the execution of the "Transform"
    or the "ExtractAndTransform" option in the "ETLOperation" Section.

![Text Description automatically
generated](media/image3.png)

-  After downloading the transformed data file to local, the user needs
    to add "CLINICALTRIAL" and "SITE" node files into the transformed
    data files' folder since these two node files can not be obtained
    from the database.
-  After manually adding two files to the transformed data files
    folder, the user must upload all the data files to a specific s3
    bucket for uploading data to the GMB website.

- After uploading the transformed data to the S3 bucket, the user
    should open the GMB-Dataloader pipeline in Jenkins to upload the
    data to the GMB website.

![Graphical user interface, application, email Description automatically
generated](media/image4.png)

- The above picture is the build parameters the user should use to run
    the pipeline. After the pipeline is done, the user should check the
    console logs about how many new nodes are loaded.

![Text Description automatically
generated](media/image5.png)

- After finishing running the GMB-Dataloader pipeline, the user should
    run the GMB-Opensearch-Loader.

![Graphical user interface, text, application, email Description
automatically generated](media/image6.png)

- The above picture is the build parameters for running the
    GMB-Opensearch-Loader.
