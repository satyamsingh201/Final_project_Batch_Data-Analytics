# End-to-End Azure Data Engineering Project

This project implements an end-to-end data engineering pipeline using Azure services, starting with data ingestion from GitHub into SQL Server via Azure Data Factory. A main ADF pipeline then processes the data and loads it into Azure Data Lake Storage Gen2 (Bronze layer). The pipeline uses incremental loading with Lookup and Stored Procedure activities to ensure only new data is processed. Azure Databricks is used to transform the data from Bronze to Silver and then to Gold layers using Delta Lake. The Silver layer contains cleaned and structured data, while the Gold layer provides aggregated, business-ready insights. This architecture ensures scalability, efficiency, and production-level data processing.


## ARCHITECTURE

GitHub → ADF → SQL Server → ADF → ADLS (Bronze) → Databricks → Silver → Gold

![architecture.png.png](https://github.com/satyamsingh201/Final_project_Batch_Data-Analytics/blob/main/images/architecture.png.png)

## PIPELINES
The project uses two Azure Data Factory pipelines to implement an incremental data processing workflow. The resource_prep_pipeline ingests data from GitHub into SQL Server as a staging layer. The main_pipeline uses two Lookup activities to fetch the last load timestamp and current maximum timestamp for watermark-based incremental loading. Only new data is loaded into the ADLS Gen2 Bronze layer and then transformed using Azure Databricks into Silver and Gold layers. Finally, the watermark is updated to ensure efficient and accurate processing in subsequent runs.

## 1. resource_prep_pipeline (Data Ingestion Pipeline)
This pipeline is responsible for extracting raw data from GitHub and loading it into SQL Server, which acts as a staging layer.

### Activities Used:
&nbsp;&nbsp;&nbsp;🔹Copy Activity <br>
### Workflow:<br> 

&nbsp;&nbsp;&nbsp;-1.Connects to the GitHub dataset (CSV/JSON format).<br>
&nbsp;&nbsp;&nbsp;-2.Extracts raw data using HTTP/REST connector.<br>
&nbsp;&nbsp;&nbsp;-3.Loads the data into SQL Server staging tables.<br>
### Purpose:<br>
&nbsp;&nbsp;&nbsp;-1.Establishes a structured and reliable source for downstream processing.<br>
&nbsp;&nbsp;&nbsp;-2.Decouples raw data ingestion from transformation logic.<br>
