#!/bin/bash

# Navigate to the streamlit app directory
cd "$(dirname "$0")"

# Install dependencies if needed
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Run the Streamlit app
echo "🚀 Starting Yelp Analytics Dashboard..."
echo "📊 Dashboard will be available at: http://localhost:8501"
echo "🔄 Press Ctrl+C to stop the server"
echo ""

streamlit run app.py --server.port 8501 --server.address 0.0.0.0 