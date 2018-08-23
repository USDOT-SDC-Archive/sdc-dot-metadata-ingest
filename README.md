
# sdc-dot-waze-data-ingest
This is a lambda function developed by SDC Team for generating the metadata from an s3 key and indexing into Elasticsearch Service.

There are two primary functions serves the need for two different lambda functions:
* **bucket-handler-lambda** - generates the metadata and indexes into Elasticsearch
* **register-kibana-dashboards** - generates the default datalake visualization dashboards

<a name="toc"/>

## Table of Contents

[I. Release Notes](#release-notes)

[II. Overview](#overview)

[III. Design Diagram](#design-diagram)

[IV. Getting Started](#getting-started)

[V. Unit Tests](#unit-tests)

[VI. Support](#support)

---

<a name="release-notes"/>


## [I. Release Notes](ReleaseNotes.md)
TO BE UPDATED

<a name="overview"/>

## II. Overview
This lamda function is triggered by aws-cloudwatch rule at an interval of every two minute.It pull data for all the 53 states
of U.S.A.(California is divided into three(CA1,CA2,CA3)) and persist this data in aws s3.

<a name="design-diagram"/>

## III. Design Diagram

![sdc-dot-metadata-ingest](images/sdc-dot-waze-data-ingest.png)

<a name="getting-started"/>

## IV. Getting Started

The following instructions describe the procedure to build and deploy the lambda.

### Prerequisites
* For getting the Waze API url you must have membership of connected citizen program of waze.Once your membership is confirmed they will provide a url to which you will use to make API Call. 

---
### ThirdParty library

*NA

### Licensed softwares

*NA

### Programming tool versions

*Python 3.6


---
### Build and Deploy the Lambda

#### Environment Variables
Below are the environment variable needed :- 

SUBMISSIONS_BUCKET_NAME - {name_of_the_bucket_in_which_you_want_to_save_data_fetched_from_waze} 
WAZE_URL                - {url_you get_after_membership_of_waze_connected_citizen_program}
ELASTICSEARCH_ENDPOINT
ENVIRONMENT_NAME
PUBLISHED_BUCKET_NAME
SUBMISSIONS_BUCKET_NAME
WAZE_CURATED_COUNTS_METRIC
WAZE_SUBMISSIONS_COUNT_METRIC
WAZE_ZERO_BYTE_SUBMISSIONS_COUNT_METRIC

#### Build Process

**Step 1**: Setup virtual environment on your system by foloowing below link
https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python

**Step 2**: Crete a script file with below contents for e.g(sdc-dot-waze-data-ingest.sh)
```#!/bin/sh

cd {path_to_your_repository}/sdc-dot-metadata-ingest
zipFileName="{path_to_your_repository}/sdc-dot-metadata-ingest.zip"

echo "Zip file name is = ${zipFileName}"

zip -9 $zipFileName lambdas/*
zip -r9 $zipFileName common/*
zip -r9 $zipFileName dashboard_registry_handler_main.py.py
zip -r9 $zipFileName bucket_event_handler_main.py

cd {path_to_your_virtual_env}/python3.6/site-packages/
zip -r9 $zipFileName chardet certifi idna
```

**Step 3**: Change the permission of the script file

```
chmod u+x sdc-dot-waze-data-ingest.sh
```

**Step 4** Run the script file
./sdc-dot-metadata-ingest.sh

**Step 5**: Upload the sdc-dot-metadata-ingest.zip generated from Step 4 to a lambda function via aws console.

[Back to top](#toc)

---
<a name="unit-tests"/>

## V. Unit Tests

TO BE UPDATED

---
<a name="support"/>

## VI. Support

For any queries you can reach to support@securedatacommons.com
---
[Back to top](#toc)
