# AWS Medallion Data Lakehouse

This repository contains the implementation of a cloud-based data lakehouse on AWS, developed as part of a Digital Transformation Management internship project.

The project follows a Medallion-inspired architecture and includes infrastructure as code, data ingestion, metadata management, and data transformation and quality processes.

## Repository Structure

* `infrastructure.yaml` — AWS infrastructure defined using AWS CloudFormation.
* `custom_crawler/` — Custom crawler for discovering datasets and partitions in the Bronze layer and registering metadata in AWS Glue.
* `silver_loader/` — Data loading and transformation pipeline for processing data from the Bronze layer into the Silver layer, including data quality and validation logic.

## Technologies

* Amazon S3
* AWS Glue
* Amazon Athena
* AWS CloudFormation
* Apache Iceberg
* Node.js
* DuckDB

## Project Status

This repository is currently under development. The documentation and architecture description will be completed and updated as the project evolves.
