import plaid
from plaid.api import plaid_api
from plaid.model import TransactionsGetRequest
import json, pandas as pd
from utils.logger import get_logger
log = get_logger("PlaidExtractor")

log.info("Starting Plaid API extraction...")
df = extract_transactions("2025-01-01", "2025-01-10")
log.info(f"Extracted {len(df)} transactions successfully.")


def get_plaid_client():
    with open('config/plaid_config.json') as f:
        creds = json.load(f)
    env = creds["environment"]
    host = getattr(plaid.Environment, env.capitalize())
    configuration = plaid.Configuration(
        host=host,
        api_key={
            'clientId': creds["client_id"],
            'secret': creds["secret"]
        }
    )
    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)
    return client, creds["access_token"]

def extract_transactions(start_date, end_date):
    client, access_token = get_plaid_client()
    request = TransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date
    )
    response = client.transactions_get(request)
    transactions = response["transactions"]
    df = pd.DataFrame(transactions)
    df.to_csv("data/bronze/transactions_raw.csv", index=False)
    print(f"✅ Extracted {len(df)} transactions into Bronze layer.")
    return df

if __name__ == "__main__":
    extract_transactions("2025-01-01", "2025-01-10")
