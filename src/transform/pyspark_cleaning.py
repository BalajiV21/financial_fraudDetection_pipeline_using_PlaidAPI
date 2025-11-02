from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, from_json
from pyspark.sql.types import StructType, StructField, StringType
from utils.logger import get_logger
log = get_logger("DataCleaning")

log.info("Cleaning and flattening transaction data...")
# your existing cleaning code
log.info("✅ Cleaned & flattened data written to Silver layer.")


spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

df = spark.read.option("header", True).csv("data/bronze/transactions_raw.csv")

df = df.dropDuplicates().na.drop(subset=["transaction_id", "amount", "date"])

# --- Define schemas for nested fields ---
location_schema = StructType([
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True)
])

payment_meta_schema = StructType([
    StructField("payment_method", StringType(), True),
    StructField("reference_number", StringType(), True),
    StructField("reason", StringType(), True)
])

# --- Flatten nested fields ---
if "location" in df.columns:
    df = df.withColumn("location_json", from_json(col("location"), location_schema))
    for c in ["address", "city", "region", "postal_code", "country"]:
        df = df.withColumn(c, col("location_json")[c])

if "payment_meta" in df.columns:
    df = df.withColumn("payment_meta_json", from_json(col("payment_meta"), payment_meta_schema))
    for c in ["payment_method", "reference_number", "reason"]:
        df = df.withColumn(c, col("payment_meta_json")[c])

# --- Text cleanup ---
text_fields = ["name", "merchant_name", "category", "city", "region", "country", "payment_method"]
for c in text_fields:
    if c in df.columns:
        df = df.withColumn(c, trim(lower(col(c))))

# --- Type casting ---
df = df.withColumn("amount", col("amount").cast("double"))
df = df.withColumn("is_pending", when(col("pending") == True, 1).otherwise(0))

# --- Select final columns ---
keep_cols = [
    "transaction_id", "account_id", "name", "merchant_name", "category",
    "amount", "iso_currency_code", "date", "authorized_date",
    "payment_channel", "transaction_type", "is_pending",
    "city", "region", "country", "payment_method"
]
df = df.select([c for c in keep_cols if c in df.columns])

df.write.format("delta").mode("overwrite").save("data/silver/transactions_cleaned")

print("✅ Cleaned & flattened data written to Silver layer.")
