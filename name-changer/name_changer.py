import logging
from typing import List, Optional

from azure.storage.blob import ContainerClient

logger = logging.getLogger(__name__)


def list_blobs(
    container_client: ContainerClient, name_starts_with: Optional[str] = None
) -> List[str]:
    """Return a list of blobs."""

    return [blob["name"] for blob in container_client.list_blobs(name_starts_with)]


def name_changer(
    container_client: ContainerClient,
    old_name: str,
    new_name: str,
    remove_old_blobs: bool,
) -> None:
    """Change the name of the given blob."""

    logger.info(f"Start changing blobname from {old_name} to {new_name}.")

    blob_client_old = container_client.get_blob_client(blob=old_name)
    buffer = blob_client_old.download_blob().readall()

    # Upload blob with new name
    blob_client_new = container_client.get_blob_client(blob=new_name)
    blob_client_new.upload_blob(buffer, overwrite=True)

    # Delte old blob
    if remove_old_blobs:
        blob_client_old.delete_blob()

    logger.info(f"Succesfully changed blobname from {old_name} to {new_name}.")
    return


def change_blob_names(
    container_client: ContainerClient,
    name_starts_with: Optional[str] = None,
    remove_old_blobs: bool = True,
) -> None:
    """Change all blobs that meet the criteria."""

    # List all blobs
    all_blobs = list_blobs(container_client, name_starts_with)

    def file_name_change(blob_name: str) -> Optional[str]:
        """Generate new name based on old name."""

        if blob_name == "random-condition":
            return
        new_name = "new_name"
        return new_name

    # Generate list of tuples with old and new names
    to_change = []
    for old_name in all_blobs:
        new_name = file_name_change(old_name)
        if new_name is None:
            continue
        to_change.append((old_name, new_name))

    # Change all blobs
    for old_name, new_name in to_change:
        name_changer(container_client, old_name, new_name, remove_old_blobs)
