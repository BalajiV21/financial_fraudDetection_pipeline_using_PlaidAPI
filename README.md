
## Financial Transactions ETL & Fraud Detection Pipeline (Plaid + Azure)

An **end-to-end ETL and ML pipeline** built with the **Plaid API**, **Azure Data Factory**, **Azure Databricks**, and **Delta Lake**.
This project extracts real-time financial transactions, cleans and normalizes them using PySpark, detects anomalies (fraud) with unsupervised ML, and stores final insights in Delta tables on Azure Data Lake.

---

### 🚀 Tech Stack

| Layer                    | Tools & Services                          |
| ------------------------ | ----------------------------------------- |
| **Extraction**           | Plaid API, Python (Requests, Pandas)      |
| **Transformation**       | PySpark on Azure Databricks               |
| **Storage**              | Azure Data Lake Gen2 + Delta Lake         |
| **Orchestration**        | Azure Data Factory (or Airflow DAG)       |
| **Security**             | .env (local), Azure Key Vault (cloud)     |
| **Machine Learning**     | PySpark MLlib (K-Means anomaly detection) |
| **Logging & Validation** | Custom logger, PySpark unit tests         |


### 🧠 Pipeline Workflow

1. **Extract (Bronze Layer)**

   * Fetch transactions from the Plaid API using secure credentials from `.env`.
   * Store raw data as CSV in `/data/bronze`.

2. **Transform (Silver Layer)**

   * Use PySpark to clean and flatten nested fields like `location` and `payment_meta`.
   * Normalize text and validate data quality.

3. **Detect Fraud (Gold Layer)**

   * Train an **unsupervised K-Means model** on transaction features (`amount`, `category`, `region`, etc.).
   * Identify outlier clusters as potential fraudulent activity.

4. **Load to Delta Lake (Master)**

   * Merge the gold-layer results into a Delta Lake master table on Azure Data Lake Gen2.
   * Schedule updates using Azure Data Factory or Airflow.



### 🔐 Environment Setup

1. Create a `.env` file in the root directory:

   ```bash
   PLAID_CLIENT_ID=your_client_id
   PLAID_SECRET=your_secret
   PLAID_ACCESS_TOKEN=access-sandbox-xxx
   PLAID_ENV=sandbox

   AZURE_STORAGE_ACCOUNT=mystorageaccount
   AZURE_STORAGE_KEY=your_storage_key
   AZURE_CONTAINER=financial-transactions

   FERNET_KEY=your_generated_key
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run Plaid extraction locally:

   ```bash
   python src/extract/plaid_api_extractor.py
   ```

4. Clean and prepare data:

   ```bash
   python src/transform/pyspark_cleaning.py
   ```

5. Run ML-based fraud detection:

   ```bash
   python src/transform/fraud_detection_ml.py
   ```

6. Load to Delta Lake:

   ```bash
   python src/load/delta_loader.py
   ```



### 🧾 Logging & Testing

Logs are automatically stored in `/logs/YYYY-MM-DD.log`
Run unit tests locally:

```bash
pytest tests/
```


### 🌐 Deployment on Azure

1. Upload your code to **Azure Repos** or GitHub.
2. Create an **Azure Data Factory pipeline** to orchestrate the scripts.
3. Use **Azure Key Vault** to store secrets instead of `.env`.
4. Schedule your ETL pipeline using triggers for daily or hourly runs.
5. Visualize results using **Power BI** or **Microsoft Fabric Lakehouse**.



### 🧩 Key Learnings

* Building **secure ETL workflows** with third-party APIs.
* Handling **nested JSON** and **data normalization** with PySpark.
* Using **unsupervised ML** for anomaly detection in financial data.
* Managing **Delta Lake merges** and **data lineage** on Azure.
* Implementing **logging**, **encryption**, and **testing** for production readiness.


### 📚 References
* [Plaid API Documentation](https://plaid.com/docs/)
* [Azure Databricks Docs](https://learn.microsoft.com/en-us/azure/databricks/)
* [Delta Lake Guide](https://docs.delta.io/latest/)
* [PySpark MLlib](https://spark.apache.org/docs/latest/ml-guide.html)

