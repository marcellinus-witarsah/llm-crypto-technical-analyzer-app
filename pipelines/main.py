import argparse
from datetime import datetime
from pipelines.source_to_minio.kraken_ohlc_four_hourly import DataPipeline as SourceToMinioDataPipeline
from pipelines.minio_to_timescaledb.ohlc_four_hourly import DataPipeline as MinioToTimescaleDBDataPipeline

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        "--pair",
        type=str,
        required=True,
        help="Pair of the cryptocurrency"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="The start date to process the data in YYYY-MM-DD format"
    )

    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="The end date to process the data in YYYY-MM-DD format"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        required=True,
        help="The end date to process the data in YYYY-MM-DD format"
    )
    
    args = parser.parse_args()
    
    pair = args.pair
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    batch_size = args.batch_size
    
    
    # Load data from local file to MinIO
    pipeline = SourceToMinioDataPipeline(
        pair=pair, 
        start_date=start_date, 
        end_date=end_date
    )
    pipeline.run()
    
    # Load from MinIO to TimescaleDB
    pipeline = MinioToTimescaleDBDataPipeline(
        pair=pair,
        batch_size=batch_size,
        start_date=start_date, 
        end_date=end_date
    )
    pipeline.run()