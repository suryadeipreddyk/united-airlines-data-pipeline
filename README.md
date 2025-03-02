# United Airlines Data Engineering Project  

This project automates the ingestion, transformation, and analysis of flight data using AWS services. It processes raw flight data, enriches it with airport details, and loads it into **Amazon Redshift** for advanced analytics. The pipeline is fully automated using **AWS Step Functions** and triggered by **EventBridge** when new data is uploaded to S3.

For a detailed breakdown of the project, visit the [**United Airlines Data Engineering Documentation**](https://devengine.notion.site/United-Airlines-Data-Engineering-Pipeline-1a432fa580888090ba20c239d1ef1a6e?pvs=4).

---

## **Project Overview**  
The project is a robust and scalable data pipeline designed to automate the ingestion, transformation, and analysis of flight data using AWS services. The project begins with raw flight data stored in an **S3 bucket**, which is cataloged and processed using **AWS Glue**. The data is enriched with airport dimension details, filtered for specific criteria, and transformed into a structured format. The processed data is then loaded into **Amazon Redshift**, a cloud data warehouse. This pipeline ensures that flight data is efficiently processed and made available for real-time querying and analysis.

The entire workflow is orchestrated using **AWS Step Functions**, which automates the execution of **Glue Crawlers** and **ETL jobs**. An **EventBridge rule** triggers the pipeline whenever a new **`.csv`** file is uploaded to the **S3 bucket**, ensuring real-time data processing. Notifications for pipeline success or failure are sent via **SNS**, enabling stakeholders to monitor the pipeline and respond to issues promptly. By leveraging **AWS services** like S3, Glue, Redshift, Step Functions, and EventBridge, this project delivers a scalable, automated, and efficient solution for managing and analyzing large-scale flight data in the cloud.

---

## **Architecture Diagram**  
Below is the architecture diagram for the project:  

![United Airlines Data Engineering Architecture](docs/architecture.png)  

---

For a detailed breakdown of the project, visit the [**United Airlines Data Engineering Documentation**](https://devengine.notion.site/United-Airlines-Data-Engineering-Pipeline-1a432fa580888090ba20c239d1ef1a6e?pvs=4).

---
