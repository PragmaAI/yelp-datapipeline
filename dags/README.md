# Airflow DAGs for Yelp Data Pipeline

This directory contains Airflow DAGs for the Yelp data processing pipeline.

## Available DAGs

### 1. `json_to_parquet_conversion`
Converts Yelp JSON data files to Parquet format for efficient processing.

**Tasks:**
- `convert_business_json`: Converts `business.json` to `business.parquet`
- `convert_review_json`: Converts `review.json` to multiple parquet chunks
- `validate_conversion`: Validates that all files were created successfully

**Input:** JSON files in `/app/data/raw/`
**Output:** Parquet files in `/app/data/raw/`

### 2. `rust_datafusion_transform`
Runs Rust DataFusion transformations to analyze the Yelp data.

**Tasks:**
- `check_input_files`: Validates that required parquet files exist (supports multiple review part files)
- `check_rust_environment`: Verifies Rust/Cargo is available
- `build_rust_transform`: Compiles the Rust code to optimized executable
- `run_rust_transform`: Executes the compiled Rust transformation
- `validate_output_files`: Checks that transformation outputs were created

**Input:** 
- Business parquet file from the JSON conversion DAG
- Multiple review part files using wildcard pattern (`review_part_*.parquet`)
**Output:** Processed analysis files in `/app/data/processed/`

## Usage

### Running via Airflow DAGs
```bash
# Run the entire pipeline using Airflow DAGs
./run_pipeline.sh --airflow
```

### Manual DAG Execution
```bash
# Trigger JSON to Parquet conversion
airflow dags trigger json_to_parquet_conversion

# Trigger Rust transformation (after JSON conversion completes)
airflow dags trigger rust_datafusion_transform
```

### Direct Execution (Original Method)
```bash
# Run pipeline directly without Airflow
./run_pipeline.sh
```

## Dependencies

- **JSON to Parquet DAG**: No dependencies, can run independently
- **Rust Transform DAG**: Depends on JSON to Parquet DAG completion

## Multi-File Support

The Rust transformation now supports reading multiple review part files:

### Input File Patterns
- **Business data**: `business.parquet` (single file)
- **Review data**: `review_part_*.parquet` (wildcard pattern for multiple files)

### Benefits
- 📁 **Scalable**: Automatically processes all available review chunks
- 🔄 **Flexible**: Works with any number of review part files
- ⚡ **Efficient**: DataFusion handles parallel reading of multiple files
- 📊 **Complete**: Processes entire review dataset regardless of chunking

### File Discovery
The DAG automatically discovers and validates all review part files:
```python
review_pattern = "/app/data/raw/review_part_*.parquet"
review_files = glob.glob(review_pattern)
```

## Performance Optimization

The Rust transformation DAG includes a build step that:
1. **Compiles** the Rust code to an optimized release executable (`cargo build --release`)
2. **Runs** the compiled executable instead of interpreting the code
3. **Reduces** execution time significantly, especially in Airflow environments

**Benefits:**
- ⚡ **Faster execution** - No compilation overhead during transformation
- 🔧 **Better error handling** - Build errors caught early
- 📦 **Optimized binary** - Release mode with performance optimizations
- 🔄 **Reusable executable** - Compiled once, run multiple times

## Output Files

The Rust transformation DAG creates the following analysis files:

1. `philadelphia_top.parquet` - Top Philadelphia businesses with review analysis
2. `city_comparison.parquet` - Cross-city comparison analysis
3. `high_rated_businesses.parquet` - High-rated businesses across all cities

## Monitoring

- Check Airflow UI for DAG execution status and logs
- Monitor task dependencies and execution order
- Review task logs for detailed execution information
- Build logs show compilation progress and any errors
- Input validation shows discovered review files and their sizes

## Configuration

Both DAGs are configured for manual triggering (`schedule_interval=None`) and can be triggered on-demand through the Airflow UI or CLI.

## Requirements

- **Rust/Cargo**: Must be installed and available in PATH
- **DataFusion**: Rust dependencies will be automatically downloaded during build
- **Disk Space**: Sufficient space for compiled executable (~10-50MB)
- **Review Files**: At least one review part file must exist matching the pattern 