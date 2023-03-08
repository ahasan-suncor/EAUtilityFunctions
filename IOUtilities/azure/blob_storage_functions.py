import pandas as pd
import os
from datetime import datetime, timedelta
from azure.storage.blob import generate_container_sas, BlobServiceClient, ContainerSasPermissions
from azure.identity import DefaultAzureCredential
from io import StringIO, BytesIO

def create_azure_storage_blob_client(storage_account_name: str) -> BlobServiceClient:
    """
    Create BlobServiceClient object to interact with named storage account

    Args:
        storage_name: The name of the Azure storage account 

    Returns:
        A BlobServiceClient

    Assumptions:
        Uses the DefaultAzureCredential() to authenticate - this will autowork in Azure compute but locally
        you need to have logged in with cli, vs code or change to interactive browser type
    """
    # set interactive_browser_credential to False if you are not authenticated in cli or vs code
    default_credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)

    # BlobServiceClient allows interaction with authorized specified container
    storage_url = f'https://{storage_account_name}.blob.core.windows.net/'
    blob_service_client = BlobServiceClient(storage_url, credential=default_credential)

    return blob_service_client

def load_csv_data_azure(blob_service_client: object,
                        container_name: str, 
                        blob_path: str) -> pd.DataFrame:
    """
    Read csv direct from Azure blob storage to Pandas DataFrame

    Args:
        blob_service_client: A BlobServiceClient object that is authenticated to the storage
        container_name: The name of the Azure storage account container
        blob_path: The path to the file in the container to download

    Returns:
        A Pandas DataFrame
    """
    # point to specific csv to fetch and download csv to memory
    blob_client_instance = blob_service_client.get_blob_client(container_name, blob_path)
    blob_target = blob_client_instance.download_blob().content_as_text()

    # StringIO creates a string buffer in memory which can be read like a file - nothing on disk which is better for deployment :)
    data = pd.read_csv(StringIO(blob_target)) 

    return data

def load_excel_data_azure(blob_service_client: object,
                          container_name: str, 
                          blob_path: str) -> pd.DataFrame:
    """
    Read excel data direct from Azure blob storage to Pandas DataFrame

    Args:
        blob_service_client: A BlobServiceClient object that is authenticated to the storage
        container_name: The name of the Azure storage account container
        blob_path: The path to the file in the container to download

    Returns:
        A Pandas DataFrame
    """    
    # point to specific excel to fetch and download to memory as bytes
    blob_client_instance = blob_service_client.get_blob_client(container_name, blob_path)
    blob_target = blob_client_instance.download_blob().content_as_bytes()

    data = pd.read_excel(blob_target, engine='openpyxl')

    return data

def load_parquet_data_azure(blob_service_client: object,
                            container_name: str, 
                            blob_path: str) -> pd.DataFrame:
    """
    Read parquet data direct from Azure blob storage to Pandas DataFrame

    Args:
        blob_service_client: A BlobServiceClient object that is authenticated to the storage
        container_name: The name of the Azure storage account container
        blob_path: The path to the file in the container to download

    Returns:
        A Pandas DataFrame
    """  
    # point to specific excel to fetch and download to memory as bytes
    blob_client_instance = blob_service_client.get_blob_client(container_name, blob_path)

    # read parquet blob into memory
    stream = BytesIO()
    blob_client_instance.download_blob().readinto(stream)
    data = pd.read_parquet(stream, engine='pyarrow')

    # for some reason parquet data read into pandas doesn't retain dtype so everything is an object type - conversion to numeric
    # is needed in order to plot, guess and check....
    for column in data:
        try:
            data[column] = pd.to_numeric(data[column])
        except:
            data[column] = data[column]

    return data

def generate_azure_blob_sas_token(blob_service_client: object, 
                                  container_name: str,
                                  key_expire_time: str = None) -> object:
    """
    Create Azure blob time expiring SAS token at container level

    Args:
        blob_service_client: A BlobServiceClient object that is authenticated to the storage
        container_name: The name of the Azure storage account container
        key_expire_time [optional]: Time in seconds from now for key to expire

    Returns:
        A SAS Token
    """  
    container_client = blob_service_client.get_container_client(container=container_name)

    # defaults to 1 hour
    if not key_expire_time:
        key_expire_time = 3600

    # create time expiring key at the container level
    udk = blob_service_client.get_user_delegation_key(
        key_start_time=datetime.utcnow() - timedelta(seconds=60),
        key_expiry_time=datetime.utcnow() + timedelta(seconds=key_expire_time))

    # create token using key
    sas_token = generate_container_sas(
        container_client.account_name,
        container_client.container_name,
        user_delegation_key=udk,
        permission=ContainerSasPermissions(read=True),
        expiry = datetime.utcnow() + timedelta(seconds=key_expire_time),
    )

    return sas_token

def create_azure_blob_sas_url(sas_token: object,
                              container_name: str,
                              blob_path: str,
                              storage_account_name: str) -> str:
    """
    Create URL to specific blob using SAS Token

    Args:
        sas_token: A time expiring SAS token
        container_name: The name of the Azure storage account container
        blob_path: The path to the file in the container to download
        storage_name: The name of the Azure storage account 

    Returns:
        A URL with SAS token appended
    """  
    storage_url = f'https://{storage_account_name}.blob.core.windows.net/'

    blob_url = os.path.join(storage_url, f'{container_name}/', f'{blob_path}' + '?' + sas_token)

    return blob_url
