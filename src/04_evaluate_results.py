from pyspark.sql.functions import col, desc, avg, round
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import get_spark_session, LAKEHOUSE_DIR

def evaluate_gold_layer():
    spark = get_spark_session("Lakehouse_Validator")
    spark.sparkContext.setLogLevel("ERROR")
    
    gold_path = f"{LAKEHOUSE_DIR}/gold"
    
    if not os.path.exists(gold_path):
        print(f"Gold layer not found at {gold_path}. Run pipeline first.")
        return

    print("\n--- GOLD LAYER METRICS ---")
    df = spark.read.format("delta").load(gold_path)
    
    # 1. Bucket Distribution
    print("\n[1] Intent Bucket Distribution:")
    df.groupBy("intent_bucket").count().orderBy(desc("count")).show()
    
    # 2. Top High-Intent Sessions
    print("\n[2] Top 5 High-Intent Sessions (Actionable Leads):")
    df.filter(col("intent_bucket") == "HIGH") \
      .select("window", "user_session", "depth", "intent_score", "next_best_action") \
      .orderBy(desc("intent_score")) \
      .limit(5) \
      .show(truncate=False)

    # 3. Correlation Check
    print("\n[3] Average Score by Depth:")
    df.groupBy("depth") \
      .agg(round(avg("intent_score"), 4).alias("avg_score")) \
      .orderBy("depth") \
      .show()

if __name__ == "__main__":
    evaluate_gold_layer()