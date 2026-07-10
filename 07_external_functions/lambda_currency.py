"""
07_external_functions/lambda_currency.py
=========================================
Local simulation of:
  1. AWS Lambda function (lambda_function.py) — PKR currency conversion
  2. Snowflake External Function calling that Lambda via API Gateway

Pakistani context:
  - Converts PKR to USD, EUR, AED, SAR (common Pakistani remittance currencies)
  - Uses a local mock exchange rate store instead of live API (offline-safe)

Snowflake External Function SQL (reference):
    CREATE EXTERNAL FUNCTION pkr_currency_convert(from_currency VARCHAR, to_currency VARCHAR)
        RETURNS VARIANT
        API_INTEGRATION = aws_pk_api_integration
        AS 'https://{api-id}.execute-api.ap-south-1.amazonaws.com/prod/pkr-convert';
"""

import json
from datetime import datetime
from typing import Union

# ── Mock exchange rates (PKR base, updated periodically) ──────────────────────
# In production this would call open.er-api.com or SBP (State Bank of Pakistan) API
MOCK_RATES_FROM_PKR = {
    "USD": 0.00360,   # 1 PKR = 0.0036 USD  (rate: ~278 PKR/USD)
    "EUR": 0.00330,
    "GBP": 0.00284,
    "AED": 0.01322,   # UAE Dirham — common for Pakistani expats
    "SAR": 0.01350,   # Saudi Riyal — large Pakistani diaspora
    "CNY": 0.02605,   # China — CPEC trade partner
    "PKR": 1.0,
}

MOCK_RATES_TO_PKR = {v_cur: 1.0 / rate for v_cur, rate in MOCK_RATES_FROM_PKR.items()}


def get_exchange_rate(from_currency: str, to_currency: str) -> float:
    """
    Simulate: https://open.er-api.com/v6/latest/{from_currency}
    Returns the exchange rate from from_currency to to_currency.
    """
    fc = from_currency.upper()
    tc = to_currency.upper()

    if fc == "PKR":
        return MOCK_RATES_FROM_PKR.get(tc, 0.0)
    elif tc == "PKR":
        return MOCK_RATES_TO_PKR.get(fc, 0.0)
    else:
        # Cross rate via PKR
        fc_to_pkr = MOCK_RATES_TO_PKR.get(fc, 0.0)
        pkr_to_tc = MOCK_RATES_FROM_PKR.get(tc, 0.0)
        return round(fc_to_pkr * pkr_to_tc, 6)


# ── Lambda handler — mirrors Section 9 lambda_function.py ────────────────────

def lambda_handler(event: dict, context=None) -> dict:
    """
    AWS Lambda handler for Snowflake External Function.

    Expected Snowflake request format:
        {
          "data": [
            [0, "PKR", "USD"],
            [1, "PKR", "AED"],
            [2, "USD", "PKR"]
          ]
        }
    """
    status_code = 200
    results = []

    try:
        body    = event.get("body", event)
        payload = json.loads(body) if isinstance(body, str) else body
        rows    = payload.get("data", [])

        for row in rows:
            row_number    = row[0]
            from_currency = row[1]
            to_currency   = row[2]

            rate = get_exchange_rate(from_currency, to_currency)
            results.append([row_number, {"exchange_rate": rate, "timestamp": datetime.utcnow().isoformat()}])

        response_body = json.dumps({"data": results})

    except Exception as err:
        status_code   = 400
        response_body = json.dumps({"error": str(err)})

    return {
        "statusCode": status_code,
        "body": response_body,
    }


# ── Snowflake External Function simulation ────────────────────────────────────

def pkr_currency_convert(from_currency: str, to_currency: str) -> dict:
    """
    Simulates calling the Snowflake External Function locally.

    In Snowflake this would be:
        SELECT pkr_currency_convert('PKR', 'USD')[0]['exchange_rate'] as rate;
    """
    mock_event = {
        "data": [[0, from_currency, to_currency]]
    }
    response = lambda_handler(mock_event)
    result   = json.loads(response["body"])
    return result["data"][0][1] if response["statusCode"] == 200 else {}


# ── Demo ──────────────────────────────────────────────────────────────────────

def run_demo():
    print("\n💱  External Function Demo — PKR Currency Conversion\n" + "─" * 50)

    conversions = [
        ("PKR", "USD"), ("PKR", "EUR"), ("PKR", "AED"),
        ("PKR", "SAR"), ("PKR", "GBP"), ("USD", "PKR"),
        ("AED", "PKR"), ("SAR", "PKR"),
    ]

    print("\n── Single call simulation ──")
    for fc, tc in conversions:
        result = pkr_currency_convert(fc, tc)
        rate   = result.get("exchange_rate", "N/A")
        print(f"  {fc} → {tc} : {rate}")

    print("\n── Batch Lambda invocation (mirrors Snowflake External Function batch call) ──")
    batch_event = {
        "data": [[i, fc, tc] for i, (fc, tc) in enumerate(conversions)]
    }
    response = lambda_handler(batch_event)
    data     = json.loads(response["body"])["data"]
    for row in data:
        print(f"  Row {row[0]}: {row[1]}")

    print("\n✅  External function demo complete.")


if __name__ == "__main__":
    run_demo()
