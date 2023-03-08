# Azure Blob Storage Utility Functions

Functions to access data in Azure Blob Storage account. 

### Purpose
Often in deployment of models, dashboards, or anything containerized and hosted on a server there is a need to access data and read the latest. The `load_csv(), excel(), parquet()` read data direct to memory and do not download to disk which is advantageous in deployment and containerization. The `sas_token()` functions generate a time expiring URL to a blob object, this is useful if say you have a dashboard and need to download PDF or large objects, best to download direct from storage account server and not put load onto webapp. 

### Example Usage
```
import blob_storage_functions as blb

blob_service_client = blb.create_azure_storage_blob_client(storage_account_name)

# read data into memory 
df_csv = blb.load_csv_data_azure(blob_service_client, container_name, blob_path)
df_excel = blb.load_excel_data_azure(blob_service_client, container_name, blob_path)
df_parquet = blb.load_parquet_data_azure(blob_service_client, container_name, blob_path)

# generate time expiring url to blobs so as to not overload memory
sas_token = blb.generate_azure_blob_sas_token(blob_service_client, container_name)
url = blb.create_azure_blob_sas_url(sas_token, container_name, blob_path, storage_account_name)
print(url)
```
