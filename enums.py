from enum import Enum


class ConnectionTarget(Enum):
    AWS = "AWS"
    MINIO = "MINIO"
    UNKNOWN = "UNKNOWN"
