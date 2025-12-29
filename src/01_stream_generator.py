import pandas as pd
import time
import os
import shutil
import json
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import DATA_SOURCE, STREAM_INPUT

def stream_data():
    print(f"Reading source data from: {DATA_SOURCE}")
    
    if not os.path.exists(DATA_SOURCE):
        print(f"Error: Source file not found at {DATA_SOURCE}")
        return

    # Create stream input directory if it doesn't exist
    if not os.path.exists(STREAM_INPUT):
        os.makedirs(STREAM_INPUT)

    # Read the dataset (chunks for memory efficiency)
    chunk_size = 1000
    print("Starting stream simulation...")
    print("Press Ctrl+C to stop.")

    try:
        for chunk in pd.read_csv(DATA_SOURCE, chunksize=chunk_size):
            # Convert timestamp to string to ensure JSON serialization
            if 'event_time' in chunk.columns:
                chunk['event_time'] = chunk['event_time'].astype(str)
            
            # Generate a unique filename
            timestamp = int(time.time() * 1000)
            target_file = os.path.join(STREAM_INPUT, f"events_{timestamp}.json")
            
            # Write chunk to JSON
            chunk.to_json(target_file, orient='records', lines=True)
            print(f"-> Ingested batch: {len(chunk)} events")
            
            # Simulate real-time delay
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nStream simulation stopped by user.")
    except Exception as e:
        print(f"Error during streaming: {e}")

if __name__ == "__main__":
    stream_data()