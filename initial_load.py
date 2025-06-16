from pipelines.source_to_minio.kraken_ohlc import DataPipeline as SourceToMinioDataPipeline
from pipelines.bronze.ohlc import DataPipeline as MinioToTimescaleDBDataPipeline
from datetime import datetime
from src.utils.minio_ops import MinioOPS
import os

if __name__ == "__main__":
    # Load data from local file to MinIO
    file = "20131006_20250331_XXBTZUSD_ohlc_240.parquet"
    minio_ops = MinioOPS()
    minio_ops.create_bucket(os.getenv("BUCKET_NAME"))
    minio_ops.write_object(
        os.getenv("BUCKET_NAME"),
        destination_file=f"{file}",
        source_file=f"tmp/{file}",
    )
    
    
    # Load data from local file to MinIO
    pipeline = SourceToMinioDataPipeline(
        pair="XXBTZUSD", 
        interval=240,
        start_date=datetime(2025, 4, 1), 
        end_date=datetime.now()
    )
    pipeline.run()
    
    # Load from MinIO to TimescaleDB
    pipeline = MinioToTimescaleDBDataPipeline(
        pair="XXBTZUSD",
        interval=240,
        batch_size=1000,
        start_date=datetime(2013, 10, 6), 
        end_date=datetime(2025, 3, 31)
    )
    pipeline.run()
    
    pipeline = MinioToTimescaleDBDataPipeline(
        pair="XXBTZUSD", 
        interval=240,
        batch_size=1000,
        start_date=datetime(2025, 4, 1), 
        end_date=datetime.now()
    )
    pipeline.run()
    
    
    
