#!/bin/bash

echo "🚀 Starting Airflow for JSON to Parquet conversion..."
echo "=================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Build and start Airflow
echo "📦 Building and starting Airflow containers..."
docker-compose up -d --build

# Wait for Airflow to be ready
echo "⏳ Waiting for Airflow to be ready..."
sleep 30

# Check if Airflow is running
if curl -s http://localhost:8080 > /dev/null; then
    echo "✅ Airflow is running!"
    echo ""
    echo "🌐 Access Airflow Web UI:"
    echo "   URL: http://localhost:8080"
    echo "   Username: admin"
    echo "   Password: admin"
    echo ""
    echo "📋 Available DAGs:"
    echo "   - json_to_parquet_conversion (Basic conversion)"
    echo "   - advanced_json_to_parquet_conversion (Advanced conversion)"
    echo ""
    echo "🔧 To trigger a DAG:"
    echo "   1. Go to the DAGs list"
    echo "   2. Find your desired DAG"
    echo "   3. Click the 'Play' button to trigger manually"
    echo ""
    echo "📊 To monitor execution:"
    echo "   - Check the 'Graph' view to see task dependencies"
    echo "   - Check the 'Logs' for detailed output"
    echo ""
    echo "🛑 To stop Airflow:"
    echo "   docker-compose down"
else
    echo "❌ Airflow failed to start. Check the logs:"
    echo "   docker-compose logs airflow"
fi 