import shutil
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import LAKEHOUSE_DIR, STREAM_INPUT

def reset_system():
    print("WARNING: This operation will permanently delete all Lakehouse data and checkpoints.")
    print(f"Storage Target: {LAKEHOUSE_DIR}")
    print(f"Input Target:   {STREAM_INPUT}")
    
    confirm = input("Type 'yes' to confirm system reset: ")
    
    if confirm.lower() == "yes":
        # 1. Clear Lakehouse Storage
        if os.path.exists(LAKEHOUSE_DIR):
            try:
                shutil.rmtree(LAKEHOUSE_DIR)
                print(f"Successfully deleted storage directory: {LAKEHOUSE_DIR}")
            except Exception as e:
                print(f"Error deleting storage. Ensure Spark is stopped. Details: {e}")
        else:
            print("Storage directory is already clean.")

        # 2. Clear Input Stream Buffer
        if os.path.exists(STREAM_INPUT):
            try:
                files_removed = 0
                for file in os.listdir(STREAM_INPUT):
                    file_path = os.path.join(STREAM_INPUT, file)
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                        files_removed += 1
                print(f"Cleared {files_removed} files from input buffer: {STREAM_INPUT}")
            except Exception as e:
                print(f"Error clearing input buffer: {e}")
        
        print("System reset complete. You may now start a new run.")
    else:
        print("Reset operation cancelled.")

if __name__ == "__main__":
    reset_system()