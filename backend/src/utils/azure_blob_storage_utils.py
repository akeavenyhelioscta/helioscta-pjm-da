import logging
import os
from typing import Optional, Dict, Union, List
from datetime import datetime
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings, BlobClient, BlobProperties
from azure.core.exceptions import AzureError

# AZURE BLOB STORAGE CREDENTIALS (from shared .env)
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")


class AzureBlobStorageClient:
    """Client for Azure Blob Storage operations."""

    def __init__(
        self,
        connection_string: str = AZURE_STORAGE_CONNECTION_STRING,
        storage_account_name: str = AZURE_STORAGE_ACCOUNT_NAME,
        container_name: str = AZURE_CONTAINER_NAME,
    ):
        self.connection_string = connection_string
        self.storage_account_name = storage_account_name
        self.container_name = container_name

    def get_blob_service_client(self) -> BlobServiceClient:
        """Get a BlobServiceClient instance."""
        return BlobServiceClient.from_connection_string(self.connection_string)

    def get_blob_client(
        self,
        blob_name: str,
        container_name: Optional[str] = None
    ) -> BlobClient:
        """Get a BlobClient for a specific blob."""
        container = container_name or self.container_name
        service_client = self.get_blob_service_client()
        return service_client.get_blob_client(container=container, blob=blob_name)

    def upload_blob(
        self,
        data: Union[str, bytes],
        blob_name: str,
        container_name: Optional[str] = None,
        content_type: Optional[str] = None,
        content_disposition: Optional[str] = None,
        overwrite: bool = True,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """Upload data as a blob."""
        container = container_name or self.container_name

        try:
            blob_client = self.get_blob_client(blob_name, container)

            content_settings = None
            if content_type or content_disposition:
                content_settings = ContentSettings(
                    content_type=content_type,
                    content_disposition=content_disposition or "inline",
                )

            blob_client.upload_blob(
                data,
                overwrite=overwrite,
                content_settings=content_settings,
                metadata=metadata,
            )

            url = f"https://{self.storage_account_name}.blob.core.windows.net/{container}/{blob_name}"
            logging.info(f"Uploaded blob: {blob_name} to {url}")
            return url

        except AzureError as e:
            logging.error(f"Error uploading blob: {str(e)}")
            raise

    def upload_file(
        self,
        file_path: Union[str, Path],
        blob_name: Optional[str] = None,
        container_name: Optional[str] = None,
        content_type: Optional[str] = None,
        overwrite: bool = True,
    ) -> str:
        """Upload a local file as a blob."""
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if blob_name is None:
            blob_name = file_path.name

        if content_type is None:
            content_type = self._get_content_type(file_path)

        with open(file_path, 'rb') as file_data:
            return self.upload_blob(
                data=file_data.read(),
                blob_name=blob_name,
                container_name=container_name,
                content_type=content_type,
                overwrite=overwrite,
            )

    def upload_dataframe_csv(
        self,
        df: pd.DataFrame,
        blob_name: str,
        container_name: Optional[str] = None,
        overwrite: bool = True,
        include_timestamp: bool = False,
        **csv_kwargs,
    ) -> str:
        """Upload a DataFrame as CSV."""
        if include_timestamp:
            name, ext = blob_name.rsplit('.', 1) if '.' in blob_name else (blob_name, 'csv')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"{name}_{timestamp}.{ext}"

        if not blob_name.endswith('.csv'):
            blob_name = f"{blob_name}.csv"

        csv_data = df.to_csv(index=False, **csv_kwargs)

        return self.upload_blob(
            data=csv_data,
            blob_name=blob_name,
            container_name=container_name,
            content_type='text/csv',
            overwrite=overwrite,
        )

    def upload_dataframe_excel(
        self,
        df: pd.DataFrame,
        blob_name: str,
        sheet_name: str = 'Sheet1',
        container_name: Optional[str] = None,
        overwrite: bool = True,
        include_timestamp: bool = False,
    ) -> str:
        """Upload a DataFrame as Excel file."""
        import tempfile

        if include_timestamp:
            name, ext = blob_name.rsplit('.', 1) if '.' in blob_name else (blob_name, 'xlsx')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"{name}_{timestamp}.{ext}"

        if not blob_name.endswith('.xlsx'):
            blob_name = f"{blob_name}.xlsx"

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as tmp_file:
                tmp_path = tmp_file.name

                with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            url = self.upload_file(
                file_path=tmp_path,
                blob_name=blob_name,
                container_name=container_name,
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                overwrite=overwrite,
            )
            return url

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def upload_html(
        self,
        html_content: str,
        blob_name: str,
        container_name: Optional[str] = None,
        overwrite: bool = True,
        include_timestamp: bool = False,
    ) -> str:
        """Upload HTML content as a blob."""
        if include_timestamp:
            name, ext = blob_name.rsplit('.', 1) if '.' in blob_name else (blob_name, 'html')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob_name = f"{name}_{timestamp}.{ext}"

        if not blob_name.endswith('.html'):
            blob_name = f"{blob_name}.html"

        return self.upload_blob(
            data=html_content,
            blob_name=blob_name,
            container_name=container_name,
            content_type='text/html',
            overwrite=overwrite,
        )

    def download_blob(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> bytes:
        """Download a blob's content."""
        try:
            blob_client = self.get_blob_client(blob_name, container_name)
            blob_data = blob_client.download_blob()
            return blob_data.readall()
        except AzureError as e:
            logging.error(f"Error downloading blob: {str(e)}")
            raise

    def delete_blob(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> bool:
        """Delete a blob."""
        try:
            blob_client = self.get_blob_client(blob_name, container_name)
            blob_client.delete_blob()
            logging.info(f"Deleted blob: {blob_name}")
            return True
        except AzureError as e:
            logging.error(f"Error deleting blob: {str(e)}")
            raise

    def list_blobs(
        self,
        container_name: Optional[str] = None,
        name_starts_with: Optional[str] = None,
    ) -> list:
        """List blobs in a container."""
        container = container_name or self.container_name

        try:
            service_client = self.get_blob_service_client()
            container_client = service_client.get_container_client(container)
            blobs = container_client.list_blobs(name_starts_with=name_starts_with)
            return [blob.name for blob in blobs]
        except AzureError as e:
            logging.error(f"Error listing blobs: {str(e)}")
            raise

    def list_blobs_with_properties(
        self,
        name_starts_with: Optional[str] = None,
        container_name: Optional[str] = None,
    ) -> List[BlobProperties]:
        """Get blob objects with full properties (not just names)."""
        container = container_name or self.container_name

        try:
            service_client = self.get_blob_service_client()
            container_client = service_client.get_container_client(container)
            return list(container_client.list_blobs(name_starts_with=name_starts_with))
        except AzureError as e:
            logging.error(f"Error listing blobs with properties: {str(e)}")
            raise

    def get_latest_blob_url(
        self,
        name_starts_with: str,
        container_name: Optional[str] = None,
    ) -> str:
        """Get URL of the most recently modified blob matching the prefix."""
        blobs = self.list_blobs_with_properties(
            name_starts_with=name_starts_with,
            container_name=container_name,
        )

        if not blobs:
            raise ValueError(f"No blobs found with prefix: {name_starts_with}")

        latest = max(blobs, key=lambda b: b.last_modified)
        return self.get_blob_url(latest.name, container_name)

    def get_blobs_as_dicts(
        self,
        name_starts_with: Optional[str] = None,
        container_name: Optional[str] = None,
        extension_filter: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """Get blobs as list of dictionaries with name and url."""
        blobs = self.list_blobs_with_properties(
            name_starts_with=name_starts_with,
            container_name=container_name,
        )

        result = []
        for blob in blobs:
            if extension_filter and not blob.name.endswith(extension_filter):
                continue
            result.append({
                'name': blob.name,
                'url': self.get_blob_url(blob.name, container_name),
            })

        return result

    def blob_exists(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> bool:
        """Check if a blob exists."""
        try:
            blob_client = self.get_blob_client(blob_name, container_name)
            return blob_client.exists()
        except AzureError as e:
            logging.error(f"Error checking blob existence: {str(e)}")
            return False

    def get_blob_url(
        self,
        blob_name: str,
        container_name: Optional[str] = None,
    ) -> str:
        """Get the URL for a blob."""
        container = container_name or self.container_name
        return f"https://{self.storage_account_name}.blob.core.windows.net/{container}/{blob_name}"

    def _get_content_type(self, file_path: Path) -> str:
        """Determine content type from file extension."""
        import mimetypes
        content_type, _ = mimetypes.guess_type(str(file_path))
        return content_type or 'application/octet-stream'


if __name__ == "__main__":
    azure_client = AzureBlobStorageClient()
    blobs = azure_client.list_blobs()
    print(blobs)
