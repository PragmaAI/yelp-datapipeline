#!/usr/bin/env python3
"""
Test script for JSON to Parquet conversion
Run this to verify the conversion logic works before using Airflow
"""

import pandas as pd
import os
from pathlib import Path

def test_business_conversion():
    """Test converting business.json to parquet"""
    input_path = "../phase3/data/raw/business.json"
    output_path = "../phase3/data/raw/business_test.parquet"
    
    if not os.path.exists(input_path):
        print(f"❌ Input file not found: {input_path}")
        return False
    
    try:
        print(f"📖 Reading {input_path}")
        df = pd.read_json(input_path, lines=True)
        print(f"✅ Read {len(df)} rows, {len(df.columns)} columns")
        
        print(f"💾 Writing to {output_path}")
        df.to_parquet(output_path, index=False)
        print(f"✅ Successfully wrote parquet file")
        
        # Verify the file was created
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path) / (1024 * 1024)
            print(f"✅ Parquet file created: {file_size:.2f} MB")
            return True
        else:
            print(f"❌ Parquet file not created")
            return False
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def test_review_conversion():
    """Test converting review.json to parquet chunks"""
    input_path = "../phase3/data/raw/review.json"
    output_dir = "../phase3/data/raw"
    
    if not os.path.exists(input_path):
        print(f"❌ Input file not found: {input_path}")
        return False
    
    try:
        print(f"📖 Reading {input_path} in chunks")
        chunk_size = 1000
        reader = pd.read_json(input_path, lines=True, chunksize=chunk_size)
        
        chunk_count = 0
        total_rows = 0
        
        for i, chunk in enumerate(reader):
            if i >= 3:  # Limit to 3 chunks for testing
                break
                
            output_path = os.path.join(output_dir, f"review_test_part_{i}.parquet")
            chunk.to_parquet(output_path, index=False)
            
            chunk_count += 1
            total_rows += len(chunk)
            print(f"✅ Created chunk {i}: {len(chunk)} rows")
        
        print(f"✅ Created {chunk_count} chunks, total {total_rows} rows")
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def main():
    print("🧪 Testing JSON to Parquet conversion...")
    print("=" * 50)
    
    # Test business conversion
    print("\n1. Testing business.json conversion:")
    business_success = test_business_conversion()
    
    # Test review conversion
    print("\n2. Testing review.json conversion:")
    review_success = test_review_conversion()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Results:")
    print(f"Business conversion: {'✅ PASS' if business_success else '❌ FAIL'}")
    print(f"Review conversion: {'✅ PASS' if review_success else '❌ FAIL'}")
    
    if business_success and review_success:
        print("\n🎉 All tests passed! Ready to use with Airflow.")
    else:
        print("\n⚠️  Some tests failed. Check the errors above.")

if __name__ == "__main__":
    main() 