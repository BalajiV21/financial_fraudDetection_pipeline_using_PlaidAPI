from pyspark.sql import SparkSession
from src.utils.validation import validate_amounts, validate_dates

spark = SparkSession.builder.appName("TestValidation").getOrCreate()

def test_amount_validation():
    data = [(1, 100.0, "2025-01-01"), (2, -50.0, "2025-01-02")]
    df = spark.createDataFrame(data, ["transaction_id", "amount", "date"])
    assert not validate_amounts(df), "❌ Amount validation failed: negative amount not caught."

def test_date_validation():
    data = [(1, 100.0, None), (2, 50.0, "2025-01-02")]
    df = spark.createDataFrame(data, ["transaction_id", "amount", "date"])
    assert not validate_dates(df), "❌ Date validation failed: null date not caught."

if __name__ == "__main__":
    test_amount_validation()
    test_date_validation()
    print("✅ Data quality tests passed.")
