import io, json, os, datetime
import typing
import boto3
from minio import Minio  # note that this minio is the package and not the current file
from minio.notificationconfig import NotificationConfig, QueueConfig
from minio.commonconfig import CopySource
import os

__client = None


def __get_client() -> Minio:
    global __client
    if not __client:
        if (
            os.getenv("S3_ENDPOINT_LOCAL")
            and os.getenv("S3_ACCESS_KEY")
            and os.getenv("S3_SECRET_KEY")
        ):
            __client = Minio(
                os.getenv("S3_ENDPOINT_LOCAL"),
                access_key=os.getenv("S3_ACCESS_KEY"),
                secret_key=os.getenv("S3_SECRET_KEY"),
                secure=False,
            )
    if not __client:
        raise Exception("S3 not connected")
    return __client


def create_bucket(bucket: str) -> bool:
    client = __get_client()
    client.make_bucket(bucket)

    config = NotificationConfig(
        queue_config_list=[
            QueueConfig(
                "arn:minio:sqs::_:webhook",
                ["s3:ObjectCreated:*"],
                config_id="1",
            ),
        ],
    )
    client.set_bucket_notification(bucket, config)
    return True


def put_object(bucket: str, object_name: str, data: str) -> str:
    client = __get_client()

    if not bucket_exists(bucket):
        create_bucket(bucket)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(bytes(data.encode("UTF-8"))),
        length=-1,
        content_type="application/json",
        part_size=1_000_000_000,
    )
    return True


def get_object(bucket: str, object_name: str) -> str:
    client = __get_client()

    if not bucket_exists(bucket):
        return ""

    return client.get_object(bucket_name=bucket, object_name=object_name).data.decode(
        "UTF-8"
    )


def download_object(
    bucket: str,
    object_name: str,
    file_type: str,
    file_name: typing.Optional[str] = None,
) -> str:
    client = __get_client()

    if not bucket_exists(bucket):
        return ""

    if not file_name:
        file_name = f"tmpfile.{file_type}"
    if os.path.exists(file_name):
        os.remove(file_name)
    client.fget_object(bucket, object_name, file_name)
    return file_name


def upload_object(bucket: str, object_name: str, file_path: str, force: bool) -> bool:
    client = __get_client()

    if not bucket_exists(bucket):
        return False

    if object_exists(bucket, object_name):
        if force:
            delete_object(bucket, object_name)
        else:
            raise ValueError(
                "Object name in bucket already taken -- if you want to overwrite use force = True"
            )

    if not os.path.exists(file_path):
        return False

    client.fput_object(bucket, object_name, file_path)
    return True


def delete_object(bucket: str, object_name: str) -> bool:
    if not object_exists(bucket, object_name):
        return False

    client = __get_client()
    client.remove_object(bucket_name=bucket, object_name=object_name)
    return True


def create_access_link(bucket: str, object_name: str) -> str:
    client = __get_client()
    if not object_exists(bucket, object_name):
        raise FileNotFoundError(
            f"Object {object_name} couldn't be found in bucket {bucket}"
        )

    return client.get_presigned_url(
        method="GET",
        bucket_name=bucket,
        object_name=object_name,
        response_headers={"response-content-type": "application/json"},
        expires=datetime.timedelta(hours=1),
    )


def create_data_upload_link(bucket: str, object_name: str) -> str:
    client = __get_client()

    if not bucket_exists(bucket):
        create_bucket(bucket)

    return client.get_presigned_url(
        method="POST",
        bucket_name=bucket,
        object_name=object_name,
        expires=datetime.timedelta(hours=12),
    )


def create_file_upload_link(bucket: str, object_name: str) -> str:
    client = __get_client()

    if not bucket_exists(bucket):
        create_bucket(bucket)

    return client.presigned_put_object(
        bucket_name=bucket,
        object_name=object_name,
        expires=datetime.timedelta(hours=12),
    )


def get_upload_credentials_and_id(target_bucket: str) -> dict:
    if not bucket_exists(target_bucket):
        create_bucket(target_bucket)

    sts_client = boto3.client(
        "sts",
        region_name="eu-west-1",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        use_ssl=False,
    )
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PutObj",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                ],
                "Resource": f"arn:aws:s3:::{target_bucket}/*",
            },
        ],
    }
    response = sts_client.assume_role(
        RoleArn="arn:x:ignored:by:minio:",
        RoleSessionName="ignored-by-minio",
        Policy=json.dumps(policy, separators=(",", ":")),
        DurationSeconds=12000,
    )

    return response


def get_upload_credentials_and_id_with_endpoint(
    target_bucket: str, endpoint: str
) -> dict:
    if not bucket_exists(target_bucket):
        create_bucket(target_bucket)

    sts_client = boto3.client(
        "sts",
        region_name="eu-west-1",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        use_ssl=False,
    )
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PutObj",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                ],
                "Resource": f"arn:aws:s3:::{target_bucket}/*",
            },
        ],
    }
    response = sts_client.assume_role(
        RoleArn="arn:x:ignored:by:minio:",
        RoleSessionName="ignored-by-minio",
        Policy=json.dumps(policy, separators=(",", ":")),
        DurationSeconds=12000,
    )

    return response


def get_download_credentials(bucket: str, object: str) -> dict:
    if not bucket_exists(bucket):
        create_bucket(bucket)

    sts_client = boto3.client(
        "sts",
        region_name="eu-west-1",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        use_ssl=False,
    )
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GetObj",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                ],
                "Resource": f"arn:aws:s3:::{bucket}/{object}",
            },
        ],
    }
    response = sts_client.assume_role(
        RoleArn="arn:x:ignored:by:minio:",
        RoleSessionName="ignored-by-minio",
        Policy=json.dumps(policy, separators=(",", ":")),
        DurationSeconds=12000,
    )

    return response


def object_exists(bucket: str, object_name: str) -> bool:
    client = __get_client()
    try:
        client.stat_object(bucket, object_name)
        return True
    except Exception as e:
        errnum = e.args[0]
        if not "code: NoSuchKey" in errnum:
            raise e

    return False


def get_all_buckets() -> typing.Dict[str, typing.Any]:
    client = __get_client()
    buckets = client.list_buckets()
    return {b.name: b for b in buckets}


def copy_object(
    source_bucket: str, source_object: str, target_bucket: str, target_object: str
) -> bool:
    client = __get_client()
    result = client.copy_object(
        target_bucket,
        target_object,
        CopySource(source_bucket, source_object),
    )
    return True


def get_bucket_objects(bucket: str, prefix: str = None) -> typing.Dict[str, typing.Any]:
    client = __get_client()
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    return {o.object_name: o for o in objects}


def bucket_exists(bucket: str) -> bool:
    client = __get_client()
    return client.bucket_exists(bucket)


def remove_bucket(bucket: str) -> bool:
    client = __get_client()
    client.remove_bucket(bucket)
    return True
