# 🍽️ Yelp Data Pipeline & Analytics Dashboard

A comprehensive data engineering pipeline that processes Yelp dataset JSON files, transforms them using Rust for high performance, orchestrates workflows with Apache Airflow, and provides interactive analytics through a Streamlit dashboard.

## 🎯 Project Overview

This project demonstrates a modern data engineering stack for processing and analyzing Yelp business data:

- **Data Ingestion**: JSON to Parquet conversion for efficient storage
- **Data Transformation**: High-performance Rust-based data processing
- **Workflow Orchestration**: Apache Airflow DAGs for reliable pipeline execution
- **Data Visualization**: Interactive Streamlit dashboard for business insights
- **Containerization**: Docker Compose for easy deployment

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw JSON      │    │   Parquet       │    │   Analytics     │
│   Data Files    │───▶│   Conversion    │───▶│   Dashboard     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Rust Data     │
                       │   Transform     │
                       └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   Apache        │
                       │   Airflow       │
                       └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Rust (for local development)
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/PragmaAI/yelp-datapipeline.git
cd yelp-datapipeline
```

### 2. Prepare Your Data

Place your Yelp dataset JSON files in the `data/raw/` directory:

```
data/
├── raw/
│   ├── business.json
│   ├── review.json
│   ├── user.json
│   └── tip.json
└── processed/
    └── (will be created automatically)
```

## 🐳 Running with Docker Compose

### Start Airflow

```bash
# Start Airflow services
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
# Default credentials: airflow/airflow
```

### Run the Data Pipeline

1. **Navigate to Airflow UI**: http://localhost:8080
2. **Enable DAGs**: Click the toggle switch next to each DAG
3. **Trigger DAGs** in this order:
   - `json_to_parquet_dag` - Converts JSON to Parquet
   - `rust_transform_dag` - Runs Rust data transformations
   - `yelp_rolling_etl` - Performs rolling ETL operations

### Monitor Pipeline Execution

- **DAGs Tab**: View all available workflows
- **Graph View**: Visualize DAG dependencies
- **Logs**: Check task execution logs
- **XCom**: View data passed between tasks

## 📊 Streamlit Analytics Dashboard

### Start the Dashboard

```bash
# Navigate to streamlit app directory
cd streamlit_app

# Install dependencies
pip install -r requirements.txt

# Run the dashboard
./run_app.sh
# or manually:
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

### Access the Dashboard

Open your browser and navigate to: **http://localhost:8501**

## 🎨 Dashboard Features

### 📈 Dashboard Overview
- **Key Metrics**: Business counts, user engagement, elite users
- **Business Performance**: City-wise comparison charts
- **User Engagement**: Distribution analysis
- **Top Performers**: Best-rated businesses and active users

### 🏢 Business Analytics
- **Interactive Filtering**: Filter by city, category, and rating
- **Performance Metrics**: Rating distribution, review analysis
- **Category Insights**: Business category performance
- **City Comparison**: Cross-city business analysis

### 👥 User Analytics
- **User Engagement**: Activity patterns and user categories
- **Elite Users**: Analysis of elite user characteristics
- **Sentiment Analysis**: User sentiment patterns
- **User Compliments**: Recognition and engagement metrics
- **Activity Timeline**: User activity over time

## 🔧 Manual Development Setup

### Local Airflow Setup

```bash
# Install Airflow
pip install apache-airflow

# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start Airflow webserver
airflow webserver --port 8080

# Start Airflow scheduler (in another terminal)
airflow scheduler
```

### Rust Development

```bash
# Navigate to Rust project
cd scripts/transform

# Build the project
cargo build --release

# Run tests
cargo test

# Run the transform
cargo run --release
```

### Python Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# For development
pip install -r requirements-dev.txt  # if available
```

## 📁 Project Structure

```
yelp-datapipeline/
├── airflow/                 # Airflow Docker configuration
│   ├── Dockerfile
│   └── entrypoint.sh
├── dags/                    # Airflow DAGs
│   ├── json_to_parquet_dag.py
│   ├── rust_transform_dag.py
│   ├── yelp_rolling_etl.py
│   └── README.md
├── data/                    # Data storage
│   ├── raw/                 # Raw JSON files
│   └── processed/           # Processed Parquet files
├── notebooks/               # Jupyter notebooks
│   └── analysis.ipynb
├── scripts/                 # Data processing scripts
│   ├── json_to_parquet.py   # Python JSON converter
│   └── transform/           # Rust data transformer
│       ├── Cargo.toml
│       └── src/main.rs
├── streamlit_app/           # Streamlit dashboard
│   ├── app.py
│   ├── requirements.txt
│   ├── run_app.sh
│   └── README.md
├── docker-compose.yml       # Docker services
├── requirements.txt         # Python dependencies
├── run_pipeline.sh          # Pipeline runner
└── start_airflow.sh         # Airflow starter
```

## 🔄 Data Pipeline Flow

### 1. Data Ingestion
- **Input**: Yelp JSON files (business, review, user, tip)
- **Process**: Convert to Parquet format for efficient storage
- **Output**: Parquet files in `data/processed/`

### 2. Data Transformation
- **Input**: Parquet files from ingestion
- **Process**: Rust-based transformations for high performance
- **Output**: Enhanced analytics datasets

### 3. Analytics Processing
- **Input**: Transformed data
- **Process**: Generate business insights, user analytics, city comparisons
- **Output**: Analytics-ready datasets for dashboard

### 4. Visualization
- **Input**: Analytics datasets
- **Process**: Streamlit dashboard rendering
- **Output**: Interactive web interface

## 📊 Key Analytics Features

### Business Insights
- Top-performing businesses by city
- Rating distribution analysis
- Category performance comparison
- Review sentiment analysis

### User Analytics
- User engagement patterns
- Elite user characteristics
- User sentiment analysis
- Activity timeline tracking

### City Performance
- Cross-city business comparison
- Rating tier analysis
- Review volume analysis
- Business density metrics

## 🛠️ Configuration

### Environment Variables

Create a `.env` file for custom configuration:

```bash
# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Data Paths
DATA_RAW_PATH=./data/raw
DATA_PROCESSED_PATH=./data/processed
```

### Docker Configuration

The `docker-compose.yml` includes:
- **Airflow Webserver**: Web UI for DAG management
- **Airflow Scheduler**: Executes DAGs
- **PostgreSQL**: Metadata database
- **Redis**: Celery backend (if using distributed execution)

## 🔍 Troubleshooting

### Common Issues

1. **Port Conflicts**
   ```bash
   # Check if ports are in use
   lsof -i :8080  # Airflow
   lsof -i :8501  # Streamlit
   ```

2. **Permission Issues**
   ```bash
   # Fix file permissions
   sudo chown -R $USER:$USER data/
   chmod +x run_pipeline.sh start_airflow.sh
   ```

3. **Docker Issues**
   ```bash
   # Clean up Docker
   docker-compose down -v
   docker system prune -f
   ```

4. **Data Loading Errors**
   - Ensure JSON files are in `data/raw/`
   - Check file permissions
   - Verify JSON format is valid

### Logs and Debugging

```bash
# Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Streamlit logs
streamlit run app.py --logger.level debug
```

## 📈 Performance Optimization

### Rust Transformations
- **Parallel Processing**: Multi-threaded data processing
- **Memory Efficiency**: Optimized for large datasets
- **Type Safety**: Compile-time error checking

### Data Storage
- **Parquet Format**: Columnar storage for fast queries
- **Compression**: Efficient data compression
- **Partitioning**: Optimized data partitioning

### Dashboard Performance
- **Caching**: Streamlit caching for faster loading
- **Lazy Loading**: Load data on demand
- **Optimized Queries**: Efficient data filtering

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Yelp Dataset**: For providing the open dataset
- **Apache Airflow**: For workflow orchestration
- **Rust**: For high-performance data processing
- **Streamlit**: For interactive data visualization

## 📞 Support

For questions and support:
- Create an issue on GitHub
- Check the documentation in each component directory
- Review the troubleshooting section above

---

**Happy Data Engineering! 🚀** 