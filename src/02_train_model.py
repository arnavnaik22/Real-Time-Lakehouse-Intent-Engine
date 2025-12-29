from pyspark.ml.feature import Word2Vec, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col, to_timestamp, collect_list, max as max_, expr, when, sum as sum_, abs as abs_, lit
from pyspark.sql.window import Window
from pyspark.ml.functions import vector_to_array 
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import get_spark_session, DATA_SOURCE, MODEL_DIR

def train_models():
    spark = get_spark_session("Model_Trainer")
    
    print(f"Reading source data from: {DATA_SOURCE}")
    df = spark.read.option("header", "true").csv(DATA_SOURCE)

    # Preprocessing: Cast types and handle nulls
    df_clean = df.withColumn("ts", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")) \
                 .withColumn("price", col("price").cast("float")) \
                 .fillna(0.0, subset=["price"]) \
                 .filter(col("product_id").isNotNull())

    # --- Part A: Word2Vec Embeddings ---
    print("Training Word2Vec model...")
    window_spec = Window.partitionBy("user_session").orderBy("ts")
    
    # Generate ordered sequence of products
    df_sequences = df_clean.withColumn("seq_locked", collect_list("product_id").over(window_spec))
    sequences = df_sequences.groupBy("user_session").agg(max_("seq_locked").alias("product_seq"))
    
    word2vec = Word2Vec(vectorSize=32, minCount=2, inputCol="product_seq", outputCol="vectors", windowSize=5)
    w2v_model = word2vec.fit(sequences)
    
    w2v_path = os.path.join(MODEL_DIR, "v1")
    w2v_model.getVectors().write.mode("overwrite").parquet(w2v_path)
    print(f"Saved vectors to {w2v_path}")

    # --- Part B: Logistic Regression ---
    print("Training classification model (Blindfolded)...")
    
    product_vectors = w2v_model.getVectors().withColumnRenamed("word", "p_id")
    
    session_labels = df_clean.groupBy("user_session").agg(
        (max_(when(col("event_type") == "purchase", 1).otherwise(0))).alias("label")
    )

    # Predict based on View/Cart behavior only.
    df_features_input = df_clean.filter(col("event_type") != "purchase") \
        .join(product_vectors, df_clean.product_id == product_vectors.p_id, "inner") \
        .withColumn("vec_array", vector_to_array(col("vector")))

    session_features = df_features_input.groupBy("user_session").agg(
        sum_(
            expr("aggregate(vec_array, 0.0D, (acc, x) -> acc + abs(x))") * when(col("event_type") == "cart", 5.0).otherwise(1.0)
        ).alias("weighted_energy"),
        
        sum_(
            when(col("event_type") == "cart", col("price")).otherwise(0.0)
        ).alias("cart_price_sum")
    ).fillna(0.0)

    final_data = session_features.join(session_labels, "user_session")
    
    assembler = VectorAssembler(inputCols=["weighted_energy", "cart_price_sum"], outputCol="features")
    train_df = assembler.transform(final_data)

    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
    lr_model = lr.fit(train_df)

    weights = lr_model.coefficients
    intercept = lr_model.intercept
    
    print(f"Coefficients: Cart_Val={weights[1]:.4f}, Energy={weights[0]:.4f}, Intercept={intercept:.4f}")
    
    coeffs = {"energy_w": weights[0], "cart_price_w": weights[1], "intercept": intercept}
    with open(os.path.join(MODEL_DIR, "lr_coeffs.json"), "w") as f:
        json.dump(coeffs, f)
        
    spark.stop()

if __name__ == "__main__":
    train_models()