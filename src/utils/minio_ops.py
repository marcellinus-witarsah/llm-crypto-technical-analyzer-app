from minio import Minio
from src.utils.logger import logger
from dotenv import load_dotenv
import os


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
        found = self.__client.bucket_exists(bucket_name)
        if not found:
            self.__client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.error(f"Bucket '{bucket_name}' already exists.")

    def write_object(self, bucket_name: str, destination_file: str, source_file: str):
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
