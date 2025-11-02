from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from utils.logger import get_logger
log = get_logger("FraudDetection")

log.info("Loading cleaned data from Silver layer...")
# ML pipeline
log.info("⚠️ Fraud detection complete. Results saved to Gold layer.")


spark = SparkSession.builder.appName("FraudDetectionML").getOrCreate()

# Load data
df = spark.read.format("delta").load("data/silver/transactions_cleaned")

# Prepare features
numeric_cols = ["amount", "is_pending"]
categorical_cols = ["payment_method", "category", "region"]

# Encode categorical variables
for c in categorical_cols:
    if c in df.columns:
        indexer = StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        df = indexer.fit(df).transform(df)

feature_cols = numeric_cols + [f"{c}_idx" for c in categorical_cols if f"{c}_idx" in df.columns]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_vec = assembler.transform(df)

# Scale
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
df_scaled = scaler.fit(df_vec).transform(df_vec)

# Train KMeans
kmeans = KMeans(k=2, seed=42, featuresCol="scaledFeatures", predictionCol="cluster")
model = kmeans.fit(df_scaled)
result = model.transform(df_scaled)

# Identify smallest cluster = potential fraud
fraud_cluster = result.groupBy("cluster").count().orderBy("count").first()["cluster"]
fraud_df = result.filter(col("cluster") == fraud_cluster)

fraud_df.write.format("delta").mode("overwrite").save("data/gold/fraud_detected")
print("⚠️ Fraud detection complete. Results saved to Gold layer.")
