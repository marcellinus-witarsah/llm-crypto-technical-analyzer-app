import argparse
from datetime import datetime
import importlib

def get_pipeline_module(pipeline_name: str):
    try:
        data_pipeline = importlib.import_module(f"pipelines.minio_to_timescaledb.{pipeline_name}")
    except ImportError:
        print(f"Error: Could not import module '{pipeline_name}'")
    return data_pipeline


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        "--pipeline-name",
        type=str,
        required=True,
        help="Name of the pipeline"
    )
    
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
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    batch_size = args.batch_size
    
    
    # Load data from local file to MinIO
    pipeline_module = get_pipeline_module(args.pipeline_name)
    pipeline = pipeline_module.DataPipeline(
        pair=pair,
        batch_size=batch_size,
        start_date=start_date, 
        end_date=end_date
    )
    pipeline.run()