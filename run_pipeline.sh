#!/bin/bash

source ~/working/pyworkspace/py3/bin/activate

echo "🔄 Starting Yelp Data Pipeline..."

# Check if Airflow DAG option is requested
if [ "$1" = "--airflow" ]; then
    echo "☁️ Running via Airflow DAGs..."
    
    # Step 1: Trigger JSON to Parquet DAG
    echo "📦 Triggering JSON to Parquet conversion DAG..."
    airflow dags trigger json_to_parquet_conversion
    
    # Step 2: Trigger Rust transformation DAG
    echo "⚙️ Triggering Rust DataFusion transformation DAG..."
    airflow dags trigger rust_datafusion_transform
    
    echo "✅ Airflow DAGs triggered. Check Airflow UI for progress."
else
    # Direct execution (original behavior)
    echo "🔧 Running pipeline directly..."
    
    # Step 1: Convert JSON to Parquet (optional if already done)
    echo "📦 Converting JSON to Parquet..."
    python3 scripts/json_to_parquet.py

    # Step 2: Build and Run DataFusion transformation
    echo "🔨 Building Rust DataFusion transformation..."
    cd scripts/transform
    cargo build --release
    
    if [ $? -eq 0 ]; then
        echo "🚀 Running compiled Rust DataFusion transformation..."
        ./target/release/transform
    else
        echo "❌ Rust build failed!"
        exit 1
    fi
    cd ../..

    # Step 3: Visualize with Jupyter Notebook (optional)
    echo "📊 You can now open notebooks/analysis.ipynb to visualize the result."

    echo "✅ Pipeline complete."
fi
