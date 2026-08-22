# User Interaction Analytics

## Problem Statement:
Modern web and application platforms generate large volumes of user-interaction events such as logins, page views, searches, product views, cart additions, and purchases. These events need to be collected, validated, transformed, stored efficiently, and made available for analytics while maintaining appropriate data access controls.

The project addresses the challenge of building a scalable cloud-based application analytics pipeline that processes daily application-event files from ingestion through analytics, incorporates data-quality validation, uses an open table format for the data lake, and provides secure analytical access based on user roles and department-level row-level security.

## Aim
The aim of this project is to design and implement an end-to-end application analytics data pipeline using AWS, Apache Iceberg, and Snowflake that:

1. Ingests daily application-event data into Amazon S3.
1. Uses AWS Lambda for event-driven processing.
1. Uses AWS Glue and PySpark for data transformation.
1. Applies Great Expectations for data-quality validation.
1. Stores curated data using Apache Iceberg on S3.
1. Loads analytical data into Snowflake.
1. Implements Role-Based Access Control (RBAC) and Row-Level Security (RLS) in Snowflake.
1. Uses CloudWatch and Python logging for pipeline monitoring and troubleshooting.
1. Produces analytics-ready data for understanding application usage and user behavior.