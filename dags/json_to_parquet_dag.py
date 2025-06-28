from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
from pathlib import Path

@dag(
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["yelp", "json", "parquet", "conversion"]
)
def json_to_parquet_conversion():
    
    @task
    def convert_business_json():
        """Convert business.json to business.parquet"""
        input_path = "/app/data/raw/business.json"
        output_path = "/app/data/raw/business.parquet"
        
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        print(f"Converting {input_path} to {output_path}")
        df = pd.read_json(input_path, lines=True)
        df.to_parquet(output_path, index=False)
        print(f"Successfully converted business data. Shape: {df.shape}")
        return {"file": "business.parquet", "rows": len(df)}
    
    @task
    def convert_review_json(chunk_size: int = 1000, max_chunks: int = None):
        """Convert review.json to multiple parquet files"""
        input_path = "/app/data/raw/review.json"
        output_dir = "/app/data/raw"
        
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        print(f"Converting {input_path} to parquet chunks")
        
        reader = pd.read_json(input_path, lines=True, chunksize=chunk_size)
        chunk_info = []
        
        for i, chunk in enumerate(reader):
            if max_chunks and i >= max_chunks:
                print(f"Stopping at chunk {i} due to max_chunks limit")
                break
                
            output_path = os.path.join(output_dir, f"review_part_{i}.parquet")
            chunk.to_parquet(output_path, index=False)
            chunk_info.append({
                "chunk": i,
                "file": f"review_part_{i}.parquet",
                "rows": len(chunk)
            })
            print(f"Created chunk {i}: {output_path} with {len(chunk)} rows")
        
        return chunk_info
    
    @task
    def validate_conversion():
        """Validate that all files were created successfully"""
        raw_dir = "/app/data/raw"
        expected_files = ["business.parquet"]
        
        # Check for review chunks
        review_chunks = [f for f in os.listdir(raw_dir) if f.startswith("review_part_") and f.endswith(".parquet")]
        expected_files.extend(review_chunks)
        
        missing_files = []
        for file in expected_files:
            file_path = os.path.join(raw_dir, file)
            if not os.path.exists(file_path):
                missing_files.append(file)
        
        if missing_files:
            raise FileNotFoundError(f"Missing converted files: {missing_files}")
        
        print(f"Validation successful! All {len(expected_files)} files created.")
        return {"status": "success", "files_created": len(expected_files)}
    
    # Define task dependencies
    business_task = convert_business_json()
    review_task = convert_review_json(chunk_size=1000, max_chunks=5)  # Limit for development
    validation_task = validate_conversion()
    
    # Set dependencies
    business_task >> validation_task
    review_task >> validation_task

# Create the DAG instance
dag = json_to_parquet_conversion() 