from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from utils.logger import get_logger
log = get_logger("DeltaLoader")

log.info("Starting Delta Lake merge job...")
# load/merge code
log.info("✅ Master Delta Lake updated.")


spark = SparkSession.builder.appName("DeltaLoader").getOrCreate()

source_path = "data/silver/transactions_cleaned"
target_path = "abfss://financial-transactions@mystorageaccount.dfs.core.windows.net/delta/master"

silver_df = spark.read.format("delta").load(source_path)

if DeltaTable.isDeltaTable(spark, target_path):
    delta_table = DeltaTable.forPath(spark, target_path)
    delta_table.alias("t").merge(
        silver_df.alias("s"),
        "t.transaction_id = s.transaction_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    silver_df.write.format("delta").mode("overwrite").save(target_path)

print("✅ Master Delta Lake updated.")
