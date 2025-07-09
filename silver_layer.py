# USe the bronze_layer to get expenses df and budget df
import pandas as pd
import sqlite3
import os
from bronze_layer import bronze_expenses, bronze_budget
from get_latest_exchange_rate import get_latest_exchange_rate
from pandas.tseries.offsets import MonthEnd

DATA_DIRECTORY = "/Users/hajirufai/test/MSF-test/OneDrive_1_7-4-2025"
OUTPUT_DIRECTORY = "/Users/hajirufai/test/MSF-test/processed_data"


def silver_budget() -> pd.DataFrame:
    """
    Silver layer function for cleaning and transforming budget data.

    This function implements the second stage of the medallion architecture by:
    - Retrieving raw budget data from the bronze layer
    - Standardizing project IDs (KEO2 → KE02)
    - Removing unnecessary columns (version, id)
    - Creating proper date columns from year/month data
    - Converting dates to month-end format for consistency

    Data Quality Transformations:
    - Drops 'version' and 'id' columns as they're not needed for analysis
    - Standardizes project ID naming convention
    - Creates datetime objects from separate year/month columns
    - Converts all dates to month-end format using pandas MonthEnd offset

    Returns:
        pd.DataFrame: Cleaned budget DataFrame containing:
            - amount (float): Budget amount in EUR
            - department (str): Department name
            - category (str): Budget category
            - project_id (str): Standardized project identifier
            - country (str): Country name
            - date (datetime): Month-end date for the budget period

    Example:
        >>> clean_budget = silver_budget()
        >>> print(clean_budget.dtypes)
        amount        float64
        department     object
        category       object
        project_id     object
        country        object
        date    datetime64[ns]

    Note:
        - Depends on bronze_budget() function
        - All dates are converted to month-end format for consistency
        - Invalid dates are handled with errors='coerce' parameter
    """
    budget_df = bronze_budget()
    # modify KEO2 to KE02 in bronze budget df
    budget_df['project_id'] = budget_df['project_id'].replace('KEO2', 'KE02')

    # drop version column, id column
    budget_df.drop(columns=['version', 'id'], inplace=True)

    # Create a date column:
    # convert the 'year' and 'month' columns to string format
    budget_df['year'] = budget_df['year'].astype(str)
    budget_df['month'] = budget_df['month'].astype(str)

    # create a temporary date string with the 1st day of the month
    budget_df['temp_date_str'] = budget_df['year'] + '-' + budget_df['month'].str.zfill(2) + '-01'

    # convert the temporary date string to datetime objects
    budget_df['date'] = pd.to_datetime(budget_df['temp_date_str'], errors='coerce')

    # convert to the last day of the month using MonthEnd
    budget_df['date'] = budget_df['date'] + MonthEnd(1)

    # drop the intermediate columns 
    budget_df = budget_df.drop(columns=['year', 'month', 'temp_date_str'])

    return budget_df


def silver_expenses() -> pd.DataFrame:
    """
    Silver layer function for cleaning, transforming, and enriching expenses data.

    This function implements the second stage of the medallion architecture by:
    - Retrieving raw expenses data from the bronze layer
    - Removing unnecessary columns (id, currency, amount_eur)
    - Creating proper date columns from year/month data
    - Converting dates to month-end format for consistency
    - Fetching current exchange rates via external API
    - Adding exchange rate columns for currency conversion
    - Calculating EUR equivalent amounts for all expenses

    Data Quality Transformations:
    - Drops 'id', 'currency', and 'amount_eur' columns
    - Creates datetime objects from separate year/month columns
    - Converts all dates to month-end format using pandas MonthEnd offset

    Currency Conversion:
    - Fetches latest exchange rates for KES and XOF to EUR
    - Maps exchange rates based on original_currency column
    - Calculates amount_eur using: amount_local * exchange_rate
    - Uses rate of 1.0 for EUR to EUR conversion

    Returns:
        pd.DataFrame: Cleaned and enriched expenses DataFrame containing:
            - amount_local (float): Original expense amount in local currency
            - year (int): Expense year
            - month (int): Expense month
            - department (str): Department name
            - category (str): Expense category
            - project_id (str): Project identifier
            - country (str): Country name
            - original_currency (str): Original currency code (EUR, KES, XOF)
            - date (datetime): Month-end date for the expense period
            - rate (float): Exchange rate from original currency to EUR
            - amount_eur (float): Expense amount converted to EUR

    Raises:
        ConnectionError: If exchange rate API is unavailable
        ValueError: If exchange rate conversion fails

    Example:
        >>> clean_expenses = silver_expenses()
        >>> print(clean_expenses[['original_currency', 'rate', 'amount_local', 'amount_eur']].head())
        original_currency  rate  amount_local  amount_eur
        0               KES  0.0077      1000.0        7.7
        1               EUR  1.0000       500.0      500.0

    Note:
        - Depends on bronze_expenses() and get_latest_exchange_rate() functions
        - Uses current exchange rates for all historical data due to API limitations
        - All dates are converted to month-end format for consistency
        - Invalid dates are handled with errors='coerce' parameter
    """
    expenses_df = bronze_expenses()
    # Drop id and currency columns
    expenses_df.drop(columns=['id', 'currency'], inplace=True)

    # Create a date column:
    # convert the 'year' and 'month' columns to string format
    expenses_df['year'] = expenses_df['year'].astype(str)
    expenses_df['month'] = expenses_df['month'].astype(str)

    # create a temporary date string with the 1st day of the month
    expenses_df['temp_date_str'] = expenses_df['year'] + '-' + expenses_df['month'].str.zfill(2) + '-01'

    # convert the temporary date string to datetime objects
    expenses_df['date'] = pd.to_datetime(expenses_df['temp_date_str'], errors='coerce')

    # convert to the last day of the month using MonthEnd
    expenses_df['date'] = expenses_df['date'] + MonthEnd(1)

    # drop the intermediate columns 
    expenses_df = expenses_df.drop(columns=['year', 'month', 'temp_date_str'])

    # Drop amount_eur column
    expenses_df.drop(columns=['amount_eur'], inplace=True)

    # Define our rate mapping
    eur_to_eur_rate = 1.0 # EUR to EUR conversion rate is 1
    kes_to_eur_rate = get_latest_exchange_rate("KES", "EUR")
    xof_to_eur_rate = get_latest_exchange_rate("XOF", "EUR")

    rate_mapping = {
        'EUR': eur_to_eur_rate,
        'KES': kes_to_eur_rate,
        'XOF': xof_to_eur_rate
    }

    # Add rate column
    expenses_df['rate'] = expenses_df['original_currency'].map(rate_mapping)

    # Add amount_eur column
    expenses_df['amount_eur'] = expenses_df['amount_local'] * expenses_df['rate']

    return expenses_df

   