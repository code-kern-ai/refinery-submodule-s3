import json
import os
from typing import Optional, Dict, Any, Union
import re

from .connections import minio
from .connections import aws
from .enums import ConnectionTarget

ARCHIVE_BUCKET = "archive"


def get_current_target() -> ConnectionTarget:
    # aws logic still open to build
    target = os.getenv("S3_TARGET")
    if target == "AWS":
        return ConnectionTarget.AWS
    return ConnectionTarget.MINIO


def bucket_exists(bucket: str) -> bool:
    """
    Checks if a bucket exists.

    Args:
        bucket (str): s3 bucket name.

    Returns:
        True if a bucket exists
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.bucket_exists(bucket)
    elif target == ConnectionTarget.AWS:
        return aws.bucket_exists(bucket)
    elif target == ConnectionTarget.UNKNOWN:
        return False

    return False


def create_bucket(bucket: str) -> bool:
    """
    Creates an s3 bucket with the given name.

    Args:
        bucket (str): s3 bucket name (organization_id).

    Returns:
        True if a bucket was created
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.create_bucket(bucket)
    elif target == ConnectionTarget.AWS:
        return aws.create_bucket(bucket)
    elif target == ConnectionTarget.UNKNOWN:
        return False

    return False


def remove_bucket(bucket: str, recursive: bool = False) -> bool:
    """
    Removes an s3 bucket.

    Args:
        bucket (str): s3 bucket name .
        recursive (bool,optional): True -> objects will be removed prior | False -> if objects inside the bucket nothing will happen

    Returns:
        True if a bucket was removed
    """

    objects = get_bucket_objects(bucket)
    if len(objects) != 0:
        if recursive:
            for obj in objects:
                delete_object(bucket, obj)
        else:
            return False

    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.remove_bucket(bucket)
    elif target == ConnectionTarget.AWS:
        return aws.remove_bucket(bucket)
    elif target == ConnectionTarget.UNKNOWN:
        return False

    return False


def archive_bucket(
    bucket: str, prefix: Optional[str] = None, delete_existing: bool = True
) -> bool:
    """
    Archives given bucket to ARCHIVE_BUCKET.
    Caution! Deletes existing archive object with the same name.

    Args:
        bucket (str): s3 bucket name.
        prefix (str): s3 object prefix (e.g. the project id).
        delete_existing (bool): True, if given bucket should be scraped after archiving.

    Returns:
        True if bucket was archived
    """
    if not bucket_exists(bucket):
        return  # e.g. for an empty project no bucket exists yet

    if not bucket_exists(ARCHIVE_BUCKET):
        create_bucket(ARCHIVE_BUCKET)

    bucket_objects = get_bucket_objects(bucket, prefix)
    for bucket_object_name in bucket_objects.keys():
        archive_object_name = "/".join([bucket, bucket_object_name])
        if object_exists(ARCHIVE_BUCKET, archive_object_name):
            delete_object(ARCHIVE_BUCKET, archive_object_name)

        copy_object(bucket, bucket_object_name, ARCHIVE_BUCKET, archive_object_name)

        if delete_existing:
            delete_object(bucket, bucket_object_name)

    if delete_existing:
        remove_bucket(bucket)

    return True


def put_object(bucket: str, object_name: str, data: str) -> bool:
    """
    Stores string data as an s3 object in a given bucket.
    If any data needs to be stored, use upload_object to store a file.

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).
        data (str): object string data to be stored.

    Returns:
        True if a bucket was created
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.put_object(bucket, object_name, data)
    elif target == ConnectionTarget.AWS:
        return aws.put_object(bucket, object_name, data)
    elif target == ConnectionTarget.UNKNOWN:
        return False

    return False


def get_object(bucket: str, object_name: str) -> str:
    """
    Returns data from an s3 object as string data.
    If any data needs to be loaded, use fget_object to download a file.

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).

    Returns:
        UTF-8 encoded string data of the given s3 object
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.get_object(bucket, object_name)
    elif target == ConnectionTarget.AWS:
        return aws.get_object(bucket, object_name)
    elif target == ConnectionTarget.UNKNOWN:
        return None

    return None


def download_object(
    bucket: str, object_name: str, file_type: str, file_name: Optional[str] = None
) -> str:
    """
    Download an s3 object to the local (docker container) file system.

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).
        file_type (str): file extention to store with on docker container (e.g. json).

    Returns:
        file name the data was downloaded to

    Raises:
        KeyError: Raises an exception.
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.download_object(bucket, object_name, file_type, file_name)
    elif target == ConnectionTarget.AWS:
        return aws.download_object(bucket, object_name, file_type, file_name)
    elif target == ConnectionTarget.UNKNOWN:
        return None

    return None


def upload_object(
    bucket: str, object_name: str, file_path: str, force: bool = False
) -> bool:
    """
    Upload a local (docker container) file to the s3 storage.

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).
        file_path (str): local file path.
        force (bool,optional): force = True deletes the object if it's already taken

    Returns:
        True if the file was uploaded
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.upload_object(bucket, object_name, file_path, force)
    elif target == ConnectionTarget.AWS:
        return aws.upload_object(bucket, object_name, file_path, force)
    elif target == ConnectionTarget.UNKNOWN:
        return False

    return False


def delete_object(bucket: str, object_name: str) -> bool:
    """
    Deletes a given object by name

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).
        data (str): object string data to be stored.

    Returns:
        True if the object was deleted
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.delete_object(bucket, object_name)
    elif target == ConnectionTarget.AWS:
        return aws.delete_object(bucket, object_name)
    elif target == ConnectionTarget.UNKNOWN:
        return False

    return False


def create_access_link(bucket: str, object_name: str) -> str:
    """
    Creates a presigned access/download link to a given object.
    Usually used to pass data between services without external access (g.g. lf exec env).

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).

    Returns:
        presigned url

    Raises:
        FileNotFoundError: If the provided bucket/object_name combination doesn't exist
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.create_access_link(bucket, object_name)
    elif target == ConnectionTarget.AWS:
        return aws.create_access_link(bucket, object_name)
    elif target == ConnectionTarget.UNKNOWN:
        return None

    return None


# currently unused
def create_data_upload_link(bucket: str, object_name: str) -> str:
    """
    Creates a presigned url using POST.
    If the bucket doesn't exist yet it will be created.

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).

    Returns:
        presigned url

    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.create_data_upload_link(bucket, object_name)
    elif target == ConnectionTarget.AWS:
        return aws.create_data_upload_link(bucket, object_name)
    elif target == ConnectionTarget.UNKNOWN:
        return None

    return None


def create_file_upload_link(bucket: str, object_name: str) -> str:
    """
    Creates a presigned url using put_object.
    If the bucket doesn't exist yet it will be created.
    Usually used to upload data from a services without external access (g.g. lf exec env).

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).

    Returns:
        presigned url

    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.create_file_upload_link(bucket, object_name)
    elif target == ConnectionTarget.AWS:
        return aws.create_file_upload_link(bucket, object_name)
    elif target == ConnectionTarget.UNKNOWN:
        return None

    return None


def object_exists(bucket: str, object_name: str) -> bool:
    """
    Checks if a given object exists in a bucket

    Args:
        bucket (str): s3 bucket name (organization_id).
        object_name (str): s3 object name (project_id + "/" + e.g. docbin_full).

    Returns:
        True if the object was deleted
    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.object_exists(bucket, object_name)
    elif target == ConnectionTarget.AWS:
        return aws.object_exists(bucket, object_name)
    elif target == ConnectionTarget.UNKNOWN:
        return False

    return False


ESSENTIAL_CREDENTIAL_KEYS = {"bucket", "Credentials", "uploadTaskId"}


def get_upload_credentials_and_id(
    target_bucket: str,
    task_id: Optional[str] = None,
    as_dict: bool = False,
    only_essentials: bool = False,
) -> Union[str, Dict[str, Any]]:
    """
    Creates an upload task and necessary credentials and ids.

    Args:
        target_bucket (str): s3 bucket name (organization_id).
        task_id (str): upload task id .

    Returns:
        response as json

    """
    target = get_current_target()
    response = None
    if target == ConnectionTarget.MINIO:
        response = minio.get_upload_credentials_and_id(target_bucket)
    elif target == ConnectionTarget.AWS:
        response = aws.get_upload_credentials_and_id(target_bucket)
    elif target == ConnectionTarget.UNKNOWN:
        pass
    if response:
        response["bucket"] = target_bucket
        if task_id:
            response["uploadTaskId"] = task_id
        if only_essentials:
            response = {
                k: v for k, v in response.items() if k in ESSENTIAL_CREDENTIAL_KEYS
            }
            del response["Credentials"]["Expiration"]
        if not as_dict:
            response = json.dumps(response, sort_keys=True, default=str)

    return response


def get_download_credentials(bucket: str, object: str) -> dict:
    """
    Creates temporary credentials to download a file.

    Args:
        bucket (str): s3 bucket name.
        object (str): s3 object name.

    Returns:
        response as json

    """
    target = get_current_target()
    response = None
    if target == ConnectionTarget.MINIO:
        response = minio.get_download_credentials(bucket, object)
    elif target == ConnectionTarget.AWS:
        response = aws.get_download_credentials(bucket, object)
    elif target == ConnectionTarget.UNKNOWN:
        pass
    if response:
        response["objectName"] = object
        response["bucket"] = bucket
        response = json.dumps(response, sort_keys=True, default=str)

    return response


def upload_tokenizer_data(bucket: str, project_id: str, data: str) -> bool:
    """
    Uploads the jsonised docbin data to s3 storage.

    Args:
        bucket (str): s3 bucket name (organization_id).
        project_id (str): project id (used as a folder), if empty it's ignored.

    Returns:
        True if no error occurs

    """
    object_name = (project_id + "/" if project_id else "") + "docbin_full"
    if not bucket_exists(bucket):
        create_bucket(bucket)

    if object_exists(bucket, object_name):
        delete_object(bucket, object_name)

    put_object(bucket, object_name, data)
    return True


def get_all_buckets() -> Dict[str, Any]:
    """
    Reads all bucket information from the target client

    Returns:
        A Dictionary with bucket name as key

    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.get_all_buckets()
    elif target == ConnectionTarget.AWS:
        return aws.get_all_buckets()
    elif target == ConnectionTarget.UNKNOWN:
        pass

    return None


def copy_object(
    source_bucket: str, source_object: str, target_bucket: str, target_object: str
) -> bool:
    """
    Copies an object to another location

    Args:
        source_bucket (str): s3 source bucket name.
        source_object (str): s3 source object name.
        target_bucket (str): s3 target bucket name.
        target_object (str): s3 target object name.

    Returns:
        True if nothing bad happens

    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.copy_object(
            source_bucket, source_object, target_bucket, target_object
        )
    elif target == ConnectionTarget.AWS:
        return aws.copy_object(
            source_bucket, source_object, target_bucket, target_object
        )
    elif target == ConnectionTarget.UNKNOWN:
        pass

    return None


def get_bucket_objects(bucket: str, prefix: str = None) -> Dict[str, Any]:
    """
    scans the s3 bucket for matching objects

    Args:
        bucket (str): s3 bucket name.
        prefix (str): only return with matching prefix, default None.

    Returns:
        Dictinaly with objects

    """
    target = get_current_target()
    if target == ConnectionTarget.MINIO:
        return minio.get_bucket_objects(bucket, prefix)
    elif target == ConnectionTarget.AWS:
        return aws.get_bucket_objects(bucket, prefix)
    elif target == ConnectionTarget.UNKNOWN:
        pass

    return None


def empty_storage(force: bool = False, only_uuid: bool = True) -> bool:
    """
    Clears the whole s3 storage.
    Should only be used locally.

    Args:
        force (bool): actually execute.
        only_uuid (bool): Only delete buckets that are uuids so e.g. Archive & kernai-terraform are kept

    Returns:
        force
    """
    if force:
        for bucket in get_all_buckets():
            if only_uuid and not __is_uuid(bucket):
                continue
            for object in get_bucket_objects(bucket):
                delete_object(bucket, object)
            remove_bucket(bucket)

    return force


def transfer_bucket_from_minio_to_aws(
    bucket: str, remove_from_minio: bool = False, force_overwrite: bool = False
) -> bool:
    """
    Transfers the files form a minio bucket to aws
    For this the files will be downloaded locally and then uploaded to aws

    Args:
        bucket (str): the name of the bucket to be transferred.
        remove_from_minio (bool, optional): True -> files & bucket will be removed from minio
        force_overwrite (bool, optional): True -> if the object name already exists on aws it will be deleted prior

    Returns:
        True if everything worked out
    """
    if not minio.bucket_exists(bucket):
        return False

    if not aws.bucket_exists(bucket):
        aws.create_bucket(bucket)

    objects = minio.get_bucket_objects(bucket)

    for obj in objects:
        local_file_name = minio.download_object(bucket, obj, "")
        aws.upload_object(bucket, obj, local_file_name, force_overwrite)
        os.remove(local_file_name)
        if remove_from_minio:
            minio.delete_object(bucket, obj)

    if remove_from_minio:
        minio.remove_bucket(bucket)

    return True


def __is_uuid(name: str) -> bool:
    if re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", name
    ):
        return True
    return False
