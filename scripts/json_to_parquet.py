# scripts/json_to_parquet.py
import pandas as pd
df = pd.read_json("data/raw/business.json", lines=True)
df.to_parquet("data/raw/business.parquet")

# Convert reviews (it's large, so maybe filter)
chunksize = 1_000
reader = pd.read_json("data/raw/review.json", lines=True, chunksize=chunksize)

for i, chunk in enumerate(reader):
    chunk.to_parquet(f"data/raw/review_part_{i}.parquet")
    if i == 0:
        break  # just sample for dev