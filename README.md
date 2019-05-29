[![Build Status](https://travis-ci.com/usdot-jpo-sdc/sdc-dot-metadata-ingest.svg?branch=master)](https://travis-ci.com/usdot-jpo-sdc/sdc-dot-metadata-ingest)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=usdot-jpo-sdc_sdc-dot-metadata-ingest&metric=alert_status)](https://sonarcloud.io/dashboard?id=usdot-jpo-sdc_sdc-dot-metadata-ingest)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=usdot-jpo-sdc_sdc-dot-metadata-ingest&metric=coverage)](https://sonarcloud.io/dashboard?id=usdot-jpo-sdc_sdc-dot-metadata-ingest)

# sdc-dot-metadata-ingest

This repository contains the source code for generating the metadata from an s3 key and indexing into Elasticsearch Service.

There are two primary functions serves the need for two different lambda functions:
* **bucket-handler-lambda** - generates the metadata and indexes into Elasticsearch
* **register-kibana-dashboards** - generates the default datalake visualization dashboards
