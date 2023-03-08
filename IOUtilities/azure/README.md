# Azure Blob Storage Utility Functions

Functions to access data in Azure Blob Storage account. 

### Purpose
Often in deployment of models, dashboards, or anything containerized and hosted on a server there is a need to access data and read the latest. The `load_csv(), excel(), parquet()` read data direct to memory and do not download to disk which is advantageous in deployment and containerization. The `sas_token()` functions generate a time expiring URL to a blob object, this is useful if say you have a dashboard and need to download PDF or large objects, best to download direct from storage account server and not put load onto webapp. 