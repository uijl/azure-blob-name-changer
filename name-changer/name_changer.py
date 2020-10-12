import io
from pathlib import Path
from typing import Optional, List
from azure.storage.blob import ContainerClient



def file_name_change(file: Path) -> Path:
    """"""

    for file in Path.cwd().iterdir():
        if not file.suffix == ".csv":
            continue
        else:
            directory = file.parent
            extension = file.suffix
            new_name = f"{file.stem}{extension}"

            file.rename(Path(directory, new_name))


def list_blobs(container_client: ContainerClient, name_starts_with: Optional[str] = None) -> List[str]:
    """Return a list of blobs."""

    return [blob["name"] for blob in list_blobs(container_client, name_starts_with)]


def name_changer(container_client: ContainerClient, old_name: str, new_name: str) -> None:
    """Change the name of the given blob."""
    
    blob_client = container_client.get_blob_client(blob=old_name)
    buffer = blob_client.download_blob().readall()
    
    blob_client = container_client.get_blob_client(blob=new_name)
    blob_client.upload_blob(buffer, overwrite=True)
    return