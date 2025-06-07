from minio import Minio
from utils.logger import logger

class MinioOPS:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool=False):
        self.__client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
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
            self.__client.fput_object(
                bucket_name,
                destination_file,
                source_file
            )
            logger.info(f"Object '{destination_file}' written to bucket '{bucket_name}' successfully.")
        except Exception as e:
            logger.error(f"Error occurred while writing object '{destination_file}' to bucket '{bucket_name}': {e}")