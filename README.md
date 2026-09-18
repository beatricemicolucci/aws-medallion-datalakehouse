# AWS Medallion Data Lakehouse

This repository contains the implementation of a cloud-based data lakehouse on AWS, developed as part of a Digital Transformation Management internship project.

The project follows a Medallion-inspired architecture and includes infrastructure as code, data ingestion, metadata management, data transformation, data quality processes, and data loading into the Gold layer.

## Repository Structure

* `infrastructure.yaml` — AWS infrastructure defined using AWS CloudFormation.

* `custom_crawler/` — Custom crawler for discovering datasets and partitions in the Bronze layer and registering metadata in AWS Glue.

* `silver_loader/` — Data loading and transformation pipeline for processing data from the Bronze layer into the Silver layer, including data quality and validation logic.

* `postgres_loader/` — Data loading pipeline for transferring data from the Silver layer into PostgreSQL tables in the Gold layer.

## Technologies

* Amazon S3
* AWS Glue
* Amazon Athena
* AWS CloudFormation
* Apache Iceberg
* PostgreSQL
* Node.js
* DuckDB

## Project Status

This repository is currently under development. The documentation and architecture description will be completed and updated as the project evolves.
