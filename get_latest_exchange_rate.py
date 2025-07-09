import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API Key from environment variable
API_KEY = os.getenv("EXCHANGE_RATE_API_KEY")

if not API_KEY:
    raise ValueError("EXCHANGE_RATE_API_KEY not found in environment variables. Please set it in your .env file.")

# Base URL for the ExchangeRate-API v6
BASE_URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/"


# Function to get the latest exchange rates
def get_latest_exchange_rate(base_currency: str, target_currency: str) -> float | None:
    """
    Fetches the latest exchange rate from a base currency to a target currency
    using the ExchangeRate-API. This function relies on the 'latest' endpoint
    due to free plan limitations, so historical dates are not supported.

    Args:
        base_currency (str): The three-letter currency code for the base currency (e.g., "EUR").
        target_currency (str): The three-letter currency code for the target currency (e.g., "KES").

    Returns:
        float | None: The exchange rate (1 base_currency = X target_currency), or None if an error occurs.
    """
    url = f"{BASE_URL}latest/{base_currency}"
    
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        if data.get("result") == "success":
            conversion_rates = data.get("conversion_rates")
            if conversion_rates and target_currency in conversion_rates:
                rate = conversion_rates[target_currency]
                # print(f"Fetched: 1 {base_currency} = {rate} {target_currency} (latest)") # You had this commented out
                return rate
            else:
                print(f"Error: Target currency '{target_currency}' not found in conversion rates for {base_currency}.")
                return None
        else:
            error_type = data.get("error-type", "Unknown error")
            print(f"API Error fetching rate for {base_currency} to {target_currency}: {error_type}")
            if error_type == "invalid-key":
                print("Check your API key. It might be incorrect or expired.")
            elif error_type == "usage-limit-reached":
                print("You might have hit your API request limit. Check your plan on ExchangeRate-API.com.")
            return None

    except requests.exceptions.ConnectionError as e:
        print(f"Network Error: Could not connect to the API for {base_currency} to {target_currency}. {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error: API request timed out for {base_currency} to {target_currency}. {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"An unexpected request error occurred for {base_currency} to {target_currency}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON response for {base_currency} to {target_currency}. Check API response format.")
        return None


# Example usage 
# if __name__ == "__main__":
#     print("--- Running get_latest_exchange_rate.py as a standalone script for testing ---")
#     # Convert USD to KES
#     usd_to_kes_rate = get_latest_exchange_rate("USD", "KES")
#     if usd_to_kes_rate:
#         print(f"Latest USD to KES: 1 USD = {usd_to_kes_rate} KES")
#     else:
#         print("Failed to get USD to KES rate.")

#     # Convert KES to EUR
#     kes_to_eur_rate = get_latest_exchange_rate("KES", "EUR")
#     if kes_to_eur_rate:
#         print(f"Latest KES to EUR: 1 KES = {kes_to_eur_rate} EUR")
#     else:
#         print("Failed to get KES to EUR rate.")