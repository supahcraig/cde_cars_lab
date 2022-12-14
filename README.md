# CDE Cars Lab





Most of this was inspired or directly stolen from Paul's CDE Workshop.
https://docs.google.com/document/d/1qqfII1i4spfGnhKd9rZKnpSgt9UIFE07WbuulGQxn-U/edit
(unclear if this is being maintained or what)

And also a tutorial found on the Cloudera website.
https://www.cloudera.com/tutorials/enrich-data-using-cloudera-data-engineering.html

The source data can be found within that tutorial, but is also available by cloning this repo (recommended).
https://www.cloudera.com/content/dam/www/marketing/tutorials/enrich-data-using-cloudera-data-engineering/tutorial-files.zip


## Assets:

Begin by cloning this repo to your local machine.   You will find 5 `csv` files that you will need to upload to a bucket your CDP environment has access to.  If the bucket & folder structure don't already exist, AWS will create it for you.   If you're doing this lab with other people, the prefix will help keep your assets separated from your colleagues'.


`aws s3 cp . s3://<YOUR BUCKET>/PREFIX/cde_workshop/ --recursive --exclude "*" --include "*.csv"`

I put mine into `s3://goes-se-sandbox01/cnelson2/cde-workshop/`, which is the bucket associated with the Cloudera SE Sandbox environment.



---

## Pre-Requisites

* A CDP Environment


### Create a CDP new CDE Service.  

* Default sizing/scaling ptions are fine
* enable public load balancer 
* check all available subnets
* You _can_ have it deploy a default virtual cluster, but it will deploy with Spark 2.4.8; Iceberg needs Spark 3.  Building your own Virtual Cluster once the service spins up will allow you to select Spark 3.

Wait ~90 minutes for the service to deploy.


### Create a Virtual Cluster

* Defaults for CPU & memory
* Spark Version select Spark 3.x (there ode in this repo that works on spark 2, but the iceberg step will not work)
* Enable Iceberg analytic tables

---

## Adjust the Code Slightly

Before we upload the coad we need to make a few edits.  You will also find 4 `*.py` files under either the Spark2 or Spark3 folder.   Navigate to whichever spark version your virtual cluster was created with.  We will need to make a few small edits to each file.

TODO:  turn these into CDE job arguments so you won't have to touch the code at all.

### Pre-SetupDW.py

Change the `s3BucketName` variable to the S3 path where you put your `csv` files.   *Do not include a trailing /*.  
Change `prefix` to your CDP username (or anything you want, really...just be consistent)

It should look something like this when you're done:

```
s3BucketName = "s3a://goes-se-sandbox01/cnelson2/cde-workshop"
prefix = 'cnelson2'
```


### EnrichData_ETL.py

Change `prefix` to your CDP username (or anything you want, really...just be consistent)


### Hive2Iceberg.py

Change the `s3BucketName` variable to the S3 path where you put your `csv` files.   *Do not include a trailing /*
Change `prefix` to your CDP username (or anything you want, really...just be consistent)

### Airflow-Dag.py

Change `prefix` to your CDP username (or anything you want, really...just be consistent)

*NOTE* in the Airflow code there are references to `job_name`.  The job names found here need to match the job names you're about to create in CDE.

---



# Using the CLI

### Create a resource

Remember, when you set up the CDE CLI you pointed it at the JOBS API for your virtual cluster.  Any commands you run will be for that cluster.

`cde resource create --name cli_resource`

### Upload a file to a resource

```
cde resource upload --name cli_resource --local-path Pre-SetupDW.py
cde resource upload --name cli_resource --local-path EnrichData_ETL.py
cde resource upload --name cli_resource --local-path Hive2Iceberg.py
cde resource upload --name cli_resource --local-path Airflow-Dag.py
```

### Creating a Job

```
cde job create --name cli_presetup --type spark --application-file /cli_resource/Pre-SetupDW.py
cde job create --name cli_enrich --type spark --application-file /cli_resource/EnrichData_ETL.py
cde job create --name cli_iceberg --type spark --application-file /cli_resource/Hive2Iceberg.py
cde job create --name cli_airflow --type airflow --dag-file Airflow-Dag.py --mount-1-resource cli_resource
```

### Running a Job

(currently this is not working, unlear if the command is the issue or if CDE is having problems)

`cde job run --name cli_presetup`

`cde job run --name cli_enrich`

`cde job run --name cli_iceberg`


TODO:  launch an airflow dag


---

# Using the UI


## Create a Resource

A Resources is basically a folder to hold any code objects you will want to create CDE jobs for.   You can upload all your code into a single resource that you will reference when you create a job.



### Create a Resource

* Go to Resources in the left hand navigation bar
* Click `Create Resource`
* Give it a name
* Type is files

### Add code to the Resource

* Click on your resource
* Click `Upload Files`
* Select files and add all your code 
* Click `Upload`


---


## Create a Job / Build & enrich some Hive tables

### Create a job

* Job type = Spark 
* Job name ==> this should match the job names in the Airflow code:
  * `pre-setupDW`
  * `enrich-ETL`
* Application File, choose File
  * `Select from Resource`
  * Select `Pre-SetupDW.py` from the resource you just created; `Select File`
* (don't need to touch main class, arguments, or configurations)
  * I have not found this configuration to be necessary, but Paul had it in his lab.
  * config key:  `spark.yarn.access.hadoopFileSystems`
  * config value:  `s3a://workshop8451-workshop-files,s3a://workshop8451-bucket` where those buckets correspond to where your data is?
* Click Create & Run



