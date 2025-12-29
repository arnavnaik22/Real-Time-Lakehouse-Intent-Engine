from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import sys

# Set Python execution environment to current executable to prevent worker disconnects
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Define project base paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Windows-specific Hadoop configuration
if sys.platform.startswith('win'):
    local_hadoop_dir = os.path.join(BASE_DIR, "hadoop")
    winutils_path = os.path.join(local_hadoop_dir, "bin", "winutils.exe")
    
    if not os.path.exists(winutils_path):
        # We allow execution even if winutils is missing, but warn the user
        print(f"Warning: winutils.exe not found at {winutils_path}. Windows execution might fail.")
    else:
        os.environ['HADOOP_HOME'] = local_hadoop_dir
        os.environ['PATH'] += os.pathsep + os.path.join(local_hadoop_dir, "bin")

def get_spark_session(app_name):
    """
    Initializes a Spark Session with Delta Lake support.
    Configuration is tuned for single-node execution with limited memory.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.streaming.minBatchesToRetain", "10") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.ui.showConsoleProgress", "true") \
        .config("spark.log.level", "WARN")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

# Path constants
DATA_SOURCE = os.path.join(BASE_DIR, "data", "raw_source", "data.csv")
STREAM_INPUT = os.path.join(BASE_DIR, "data", "stream_input")
LAKEHOUSE_DIR = os.path.join(BASE_DIR, "storage")
MODEL_DIR = os.path.join(BASE_DIR, "models", "product_vectors")