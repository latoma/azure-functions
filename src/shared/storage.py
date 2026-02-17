import os
from azure.storage.blob import BlobServiceClient


_blob_service_client = None


def get_blob_service_client() -> BlobServiceClient:
    """Get or create a singleton BlobServiceClient."""
    global _blob_service_client

    if _blob_service_client is None:
        connection_string = os.environ["AzureWebJobsStorage"]
        _blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    return _blob_service_client
