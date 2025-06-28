from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import glob
from pathlib import Path

@dag(
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["yelp", "rust", "datafusion", "transformation"]
)
def rust_datafusion_transform():
    
    @task
    def check_input_files():
        """Check that required input parquet files exist"""
        # Check for business file
        business_file = "/app/data/raw/business.parquet"
        if not os.path.exists(business_file):
            raise FileNotFoundError(f"Missing required input file: {business_file}")
        
        # Check for review part files using glob pattern
        review_pattern = "/app/data/raw/review_part_*.parquet"
        review_files = glob.glob(review_pattern)
        
        if not review_files:
            raise FileNotFoundError(f"No review part files found matching pattern: {review_pattern}")
        
        # Check for user file
        user_file = "/app/data/raw/user.parquet"
        if not os.path.exists(user_file):
            raise FileNotFoundError(f"Missing required input file: {user_file}")
        
        print(f"✅ Business file found: {business_file}")
        print(f"✅ Review part files found: {len(review_files)} files")
        for review_file in review_files:
            file_size = os.path.getsize(review_file)
            print(f"   - {os.path.basename(review_file)} ({file_size} bytes)")
        
        print(f"✅ User file found: {user_file}")
        user_file_size = os.path.getsize(user_file)
        print(f"   - {os.path.basename(user_file)} ({user_file_size} bytes)")
        
        return {
            "status": "success", 
            "business_file": business_file,
            "review_files": review_files,
            "user_file": user_file,
            "total_files": len(review_files) + 2
        }
    
    @task
    def check_rust_environment():
        """Check that Rust and Cargo are available"""
        import subprocess
        
        try:
            # Check if cargo is available
            result = subprocess.run(['cargo', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Rust/Cargo found: {result.stdout.strip()}")
                return {"status": "success", "cargo_version": result.stdout.strip()}
            else:
                raise RuntimeError("Cargo not found or not working properly")
        except FileNotFoundError:
            raise RuntimeError("Cargo not found in PATH. Please install Rust.")
    
    # Define the Rust build step as a BashOperator
    build_rust_task = BashOperator(
        task_id='build_rust_transform',
        bash_command="""
        cd /app/scripts/transform && \
        echo "🔨 Building Rust transformation..." && \
        cargo build --release && \
        echo "✅ Build completed successfully"
        """,
        cwd="/app/scripts/transform"
    )
    
    # Define the Rust transformation execution as a BashOperator
    run_rust_task = BashOperator(
        task_id='run_rust_transform',
        bash_command="""
        cd /app/scripts/transform && \
        echo "🚀 Running compiled Rust transformation..." && \
        ./target/release/transform && \
        echo "✅ Transformation completed successfully"
        """,
        cwd="/app/scripts/transform"
    )
    
    @task
    def validate_output_files():
        """Validate that transformation output files were created"""
        expected_files = [
            "/app/data/processed/philadelphia_top.parquet",
            "/app/data/processed/city_comparison.parquet",
            "/app/data/processed/high_rated_businesses.parquet",
            "/app/data/processed/user_engagement.parquet",
            "/app/data/processed/user_business_interactions.parquet",
            "/app/data/processed/user_sentiment.parquet",
            "/app/data/processed/user_compliments.parquet",
            "/app/data/processed/user_activity_timeline.parquet",
            "/app/data/processed/elite_users.parquet"
        ]
        
        missing_files = []
        created_files = []
        
        for file_path in expected_files:
            if os.path.exists(file_path):
                created_files.append(file_path)
                # Get file size
                file_size = os.path.getsize(file_path)
                print(f"✅ Created: {file_path} ({file_size} bytes)")
            else:
                missing_files.append(file_path)
        
        if missing_files:
            print(f"⚠️ Missing output files: {missing_files}")
            # Don't fail the task, just warn
            return {
                "status": "partial_success", 
                "files_created": len(created_files),
                "files_missing": len(missing_files),
                "missing_files": missing_files
            }
        
        print(f"✅ All transformation outputs created successfully: {len(created_files)} files")
        return {
            "status": "success", 
            "files_created": len(created_files),
            "files_missing": 0
        }
    
    # Define task dependencies
    check_input = check_input_files()
    check_rust = check_rust_environment()
    validate_output = validate_output_files()
    
    # Set dependencies: check inputs and rust env -> build -> run -> validate
    check_input >> build_rust_task
    check_rust >> build_rust_task
    build_rust_task >> run_rust_task >> validate_output

# Create the DAG instance
dag = rust_datafusion_transform() 