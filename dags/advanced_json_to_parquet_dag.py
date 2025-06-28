from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
import json
from pathlib import Path
from typing import Dict, List, Optional

@dag(
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["yelp", "json", "parquet", "advanced", "conversion"]
)
def advanced_json_to_parquet_conversion():
    
    @task
    def get_json_files():
        """Discover JSON files in the raw data directory"""
        raw_dir = "/app/data/raw"
        json_files = []
        
        if os.path.exists(raw_dir):
            for file in os.listdir(raw_dir):
                if file.endswith('.json'):
                    json_files.append({
                        'filename': file,
                        'path': os.path.join(raw_dir, file),
                        'size_mb': os.path.getsize(os.path.join(raw_dir, file)) / (1024 * 1024)
                    })
        
        print(f"Found {len(json_files)} JSON files: {[f['filename'] for f in json_files]}")
        return json_files
    
    @task
    def convert_json_file(file_info: Dict, chunk_size: int = 1000, max_chunks: Optional[int] = None):
        """Convert a single JSON file to Parquet format"""
        filename = file_info['filename']
        input_path = file_info['path']
        output_dir = "/app/data/raw"
        
        # Determine output filename
        base_name = filename.replace('.json', '')
        output_path = os.path.join(output_dir, f"{base_name}.parquet")
        
        print(f"Processing {filename} (size: {file_info['size_mb']:.2f} MB)")
        
        try:
            # For smaller files, convert directly
            if file_info['size_mb'] < 100:  # Less than 100MB
                df = pd.read_json(input_path, lines=True)
                df.to_parquet(output_path, index=False)
                print(f"Converted {filename} directly. Shape: {df.shape}")
                return {
                    'file': filename,
                    'output': f"{base_name}.parquet",
                    'rows': len(df),
                    'chunks': 1
                }
            
            # For larger files, use chunking
            else:
                reader = pd.read_json(input_path, lines=True, chunksize=chunk_size)
                chunk_info = []
                
                for i, chunk in enumerate(reader):
                    if max_chunks and i >= max_chunks:
                        print(f"Stopping at chunk {i} due to max_chunks limit")
                        break
                    
                    chunk_output_path = os.path.join(output_dir, f"{base_name}_part_{i}.parquet")
                    chunk.to_parquet(chunk_output_path, index=False)
                    chunk_info.append({
                        'chunk': i,
                        'file': f"{base_name}_part_{i}.parquet",
                        'rows': len(chunk)
                    })
                    print(f"Created chunk {i}: {chunk_output_path} with {len(chunk)} rows")
                
                return {
                    'file': filename,
                    'chunks': len(chunk_info),
                    'total_rows': sum(chunk['rows'] for chunk in chunk_info),
                    'chunk_info': chunk_info
                }
                
        except Exception as e:
            print(f"Error converting {filename}: {str(e)}")
            raise
    
    @task
    def create_conversion_summary(conversion_results: List[Dict]):
        """Create a summary of all conversions"""
        summary = {
            'total_files_processed': len(conversion_results),
            'total_rows_converted': 0,
            'files_converted': [],
            'errors': []
        }
        
        for result in conversion_results:
            if 'rows' in result:  # Direct conversion
                summary['total_rows_converted'] += result['rows']
                summary['files_converted'].append({
                    'input': result['file'],
                    'output': result['output'],
                    'rows': result['rows']
                })
            elif 'total_rows' in result:  # Chunked conversion
                summary['total_rows_converted'] += result['total_rows']
                summary['files_converted'].append({
                    'input': result['file'],
                    'chunks': result['chunks'],
                    'total_rows': result['total_rows']
                })
        
        print("Conversion Summary:")
        print(f"Files processed: {summary['total_files_processed']}")
        print(f"Total rows converted: {summary['total_rows_converted']:,}")
        print(f"Files converted: {len(summary['files_converted'])}")
        
        return summary
    
    @task
    def cleanup_old_parquet_files():
        """Remove old parquet files before conversion (optional)"""
        raw_dir = "/app/data/raw"
        removed_files = []
        
        if os.path.exists(raw_dir):
            for file in os.listdir(raw_dir):
                if file.endswith('.parquet'):
                    file_path = os.path.join(raw_dir, file)
                    os.remove(file_path)
                    removed_files.append(file)
                    print(f"Removed old parquet file: {file}")
        
        print(f"Cleaned up {len(removed_files)} old parquet files")
        return removed_files
    
    # Define the workflow
    json_files = get_json_files()
    
    # Optional: cleanup old files first
    # cleanup_task = cleanup_old_parquet_files()
    # cleanup_task >> json_files
    
    # Convert each JSON file
    conversion_results = convert_json_file.expand(
        file_info=json_files
    )
    
    # Create summary
    summary = create_conversion_summary(conversion_results)
    
    # Set dependencies
    json_files >> conversion_results >> summary

# Create the DAG instance
dag = advanced_json_to_parquet_conversion() 