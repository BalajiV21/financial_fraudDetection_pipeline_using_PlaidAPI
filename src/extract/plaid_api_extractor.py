import os
import pandas as pd
import plaid
from plaid.api import plaid_api
from plaid.model import TransactionsGetRequest
from dotenv import load_dotenv
from utils.logger import get_logger

# --- Setup ---
load_dotenv()
log = get_logger("PlaidExtractor")

PLAID_CLIENT_ID = os.getenv("PLAID_CLIENT_ID")
PLAID_SECRET = os.getenv("PLAID_SECRET")
PLAID_ACCESS_TOKEN = os.getenv("PLAID_ACCESS_TOKEN")
PLAID_ENV = os.getenv("PLAID_ENV", "sandbox")

def get_plaid_client():
    """Create a Plaid API client using environment variables."""
    env = getattr(plaid.Environment, PLAID_ENV.capitalize())
    configuration = plaid.Configuration(
        host=env,
        api_key={
            "clientId": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET
        }
    )
    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)

def extract_transactions(start_date, end_date):
    """Extract transactions from Plaid and write to Bronze layer."""
    log.info(f"Starting Plaid API extraction: {start_date} → {end_date}")

    client = get_plaid_client()
    request = TransactionsGetRequest(
        access_token=PLAID_ACCESS_TOKEN,
        start_date=start_date,
        end_date=end_date
    )

    try:
        response = client.transactions_get(request)
        transactions = response["transactions"]
        df = pd.DataFrame(transactions)
        os.makedirs("data/bronze", exist_ok=True)
        df.to_csv("data/bronze/transactions_raw.csv", index=False)
        log.info(f"✅ Extracted {len(df)} transactions successfully.")
        return df

    except Exception as e:
        log.error(f"❌ Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    extract_transactions("2025-01-01", "2025-01-10")
