import os

from dotenv import load_dotenv
from minio import Minio

from src.utils.logger import logger


class MinioOPS:
    def __init__(self):
        load_dotenv()
        self.__client = Minio(
            "localhost:9000",
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            secure=False,
        )

    def create_bucket(self, bucket_name: str):
        """Create bucket inside MinIO

        Args:
            bucket_name (str): bucket name that you want to create.
        """
        found = self.__client.bucket_exists(bucket_name)
        if not found:
            self.__client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.error(f"Bucket '{bucket_name}' already exists.")

    def write_object(self, bucket_name: str, destination_file: str, source_file: str):
        """Create an object inside the bucket

        Args:
            bucket_name (str): bucket name that you want to write.
            destination_file (str): file name where the object stored.
            source_file (str): local file.
        """
        try:
            self.__client.fput_object(bucket_name, destination_file, source_file)
            logger.info(
                f"Object '{destination_file}' written to bucket '{bucket_name}' successfully."
            )
        except Exception as e:
            logger.error(
                f"Error occurred while writing object '{destination_file}' to bucket '{bucket_name}': {e}"
            )

    def read_object(self, bucket_name: str, object_name: str) -> bytes:
        """Read object data from bucket

        Args:
            bucket_name (str): bucket name that you want to read.
            object_name (str): object name/path that you want to read.

        Returns:
            bytes: object data.
        """
        try:
            response = self.__client.get_object(bucket_name, object_name)
            data = response.data
        except Exception as e:
            logger.error(
                f"Error occurred while retriving {object_name} object from {bucket_name} bucket"
            )
        finally:
            # Need to do this so that we
            response.close()
            response.release_conn()

        return data
