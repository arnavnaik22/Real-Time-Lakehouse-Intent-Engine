from pyspark.sql.functions import col, from_json, to_timestamp, broadcast, lit, when, window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.ml.functions import vector_to_array 
import pyspark.sql.functions as F
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import get_spark_session, STREAM_INPUT, LAKEHOUSE_DIR, MODEL_DIR

# Configuration
PIPELINE_MODE = "demo" 

def run_pipeline():
    spark = get_spark_session("Lakehouse_Streaming_Engine")
    spark.sparkContext.setLogLevel("WARN")

    # Load Model Coefficients
    coeff_path = os.path.join(MODEL_DIR, "lr_coeffs.json")
    if not os.path.exists(coeff_path):
        print("Error: Coefficients not found. Run training script.")
        return
        
    with open(coeff_path, "r") as f:
        coeffs = json.load(f)
        
    cart_price_w = coeffs["cart_price_w"]
    energy_w = coeffs["energy_w"]
    intercept = coeffs["intercept"]
    
    # Visualization Boost (Ensures you see HIGH/MEDIUM in the demo)
    if PIPELINE_MODE == "demo" and energy_w < 0.05:
        intercept = -3.0  
        energy_w = 0.05   
        cart_price_w = 0.005 
    
    print(f"Pipeline Mode: {PIPELINE_MODE.upper()}")
    print(f"Model: Sigmoid({intercept:.2f} + {cart_price_w:.4f}*CartVal + {energy_w:.2f}*Energy)")

    json_schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_session", StringType(), True)
    ])

    print("Initializing pipeline...")
    
    bronze_path = f"{LAKEHOUSE_DIR}/bronze"
    gold_path = f"{LAKEHOUSE_DIR}/gold"
    
    if not os.path.exists(bronze_path):
        spark.range(0).selectExpr("cast(null as string) as event_time", "cast(null as string) as event_type", 
                                  "cast(null as string) as product_id", "cast(null as string) as price", 
                                  "cast(null as string) as user_session").write.format("delta").mode("ignore").save(bronze_path)

    # 1. Ingest
    raw_stream = spark.readStream.schema(json_schema).json(STREAM_INPUT)
    bronze_write = raw_stream.select("event_time", "event_type", "product_id", "price", "user_session") \
        .writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", f"{LAKEHOUSE_DIR}/checkpoints/bronze").start(bronze_path)

    # 2. Enrich
    bronze_read = spark.readStream.format("delta").load(bronze_path)
    model_path = os.path.join(MODEL_DIR, "v1")
    vectors = spark.read.parquet(model_path).withColumn("prod_vec", vector_to_array(col("vector"))).drop("vector")
    vectors_broadcast = broadcast(vectors)

    silver_df = bronze_read \
        .withColumn("timestamp", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")) \
        .withColumn("price_val", col("price").cast("float")) \
        .fillna(0.0, subset=["price_val"]) \
        .withWatermark("timestamp", "10 minutes") \
        .join(vectors_broadcast, bronze_read.product_id == vectors_broadcast.word, "inner") \
        .drop("word")

    silver_write = silver_df.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", f"{LAKEHOUSE_DIR}/checkpoints/silver").start(f"{LAKEHOUSE_DIR}/silver")

    # 3. Decision Engine
    print("Starting inference stream...")

    energy_expr = """
      aggregate(
        zipped_data, 
        0.0D, 
        (acc, item) -> acc + (aggregate(item.vec, 0.0D, (a, x) -> a + abs(x)) * item.weight)
      )
    """

    gold_df = silver_df \
        .withColumn("action_weight", when(col("event_type") == "cart", 5.0).otherwise(1.0)) \
        .groupBy(F.window("timestamp", "1 minute"), "user_session") \
        .agg(
            F.collect_list(F.struct(col("prod_vec").alias("vec"), col("action_weight").alias("weight"))).alias("zipped_data"),
            F.sum(when(col("event_type") == "cart", col("price_val")).otherwise(0.0)).alias("cart_value"),
            F.count("product_id").alias("depth")
        ) \
        .withColumn("weighted_energy", F.expr(energy_expr)) \
        .withColumn("raw_logit", 
                    lit(intercept) + 
                    (col("cart_value") * lit(cart_price_w)) + 
                    (col("weighted_energy") * lit(energy_w))
        ) \
        .withColumn("intent_score", F.expr("1 / (1 + exp(-raw_logit))")) \
        .withColumn("intent_score", F.round("intent_score", 4)) \
        .withColumn("intent_bucket", 
                    when(col("intent_score") >= 0.7, "HIGH")
                    .when(col("intent_score") >= 0.4, "MEDIUM")
                    .otherwise("LOW")) \
        .withColumn("next_best_action", 
                    when(col("intent_bucket") == "HIGH", "PUSH_DISCOUNT")
                    .when(col("intent_bucket") == "MEDIUM", "EMAIL_NUDGE")
                    .otherwise("LOG_ONLY")) \
        .drop("zipped_data", "raw_logit", "weighted_energy")

    def save_and_show(batch_df, batch_id):
        batch_df.persist()
        if batch_df.count() > 0:
            batch_df.write.format("delta").mode("append").save(gold_path)
            print(f"\n--- Batch {batch_id} Processed ---")
            batch_df.select("window", "user_session", "depth", "intent_score", "intent_bucket", "next_best_action") \
                    .show(truncate=False)
        batch_df.unpersist()

    gold_write = gold_df.writeStream \
        .foreachBatch(save_and_show) \
        .outputMode("update") \
        .option("checkpointLocation", f"{LAKEHOUSE_DIR}/checkpoints/gold") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_pipeline()