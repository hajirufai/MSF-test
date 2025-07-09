import pandas as pd
import sqlite3
import os

DATA_DIRECTORY = "/Users/hajirufai/test/MSF-test/OneDrive_1_7-4-2025"
OUTPUT_DIRECTORY = "/Users/hajirufai/test/MSF-test/processed_data"

def bronze_budget() -> pd.DataFrame:
    """
    Bronze layer function for ingesting budget data from CSV files.

    This function implements the first stage of the medallion architecture by:
    - Reading all budget CSV files from the data directory
    - Standardizing project IDs and country mappings
    - Combining all budget data into a single DataFrame
    - Adding metadata columns (project_id, country)

    The function processes files matching the pattern '*_budget.csv' and applies
    a standardized project-to-country mapping to ensure data consistency.

    Returns:
        pd.DataFrame: Combined budget DataFrame containing:
            - All original budget columns from CSV files
            - project_id (str): Standardized project identifier
            - country (str): Country name from PROJECT_CURRENCY_MAP

    Raises:
        FileNotFoundError: If DATA_DIRECTORY does not exist

    Example:
        >>> budget_df = bronze_budget()
        >>> print(budget_df.columns)
        Index(['amount', 'year', 'month', 'department', 'category', 'version', 'id', 'project_id', 'country'])

    Note:
        - Only processes files ending with '_budget.csv'
        - Skips projects not found in PROJECT_CURRENCY_MAP
        - Returns empty DataFrame if no valid files are found
    """

    # This dictionary contains the TRUTH for project country and currency,
    # overriding any inconsistencies found in source data.
    PROJECT_CURRENCY_MAP = {
        "BE01": {"country": "Belgium", "currency": "EUR"},
        "BE55": {"country": "Belgium", "currency": "EUR"},
        "KE01": {"country": "Kenya", "currency": "KES"},   # Corrected: Force KES for KE01 expenses, overriding DB's XOF
        "KEO2": {"country": "Kenya", "currency": "KES"},
        "SN01": {"country": "Senegal", "currency": "XOF"}, # Corrected: Force Senegal for SN01 country, overriding DB's Kenya
        "SN02": {"country": "Senegal", "currency": "XOF"},
        "BF01": {"country": "Burkina Faso", "currency": "XOF"},
        "BF02": {"country": "Burkina Faso", "currency": "XOF"},
    }

    DATA_DIRECTORY = "/Users/hajirufai/test/MSF-test/OneDrive_1_7-4-2025"
    OUTPUT_DIRECTORY = "/Users/hajirufai/test/MSF-test/processed_data"

    # Read csvs (budget data)
    import pandas as pd
    import os

    # --- Function to extract budget from a single CSV ---
    def extract_budget_from_csv(csv_path: str) -> pd.DataFrame:
        """
        Extract budget data from a single CSV file.

        Args:
            csv_path (str): Full path to the CSV file to read

        Returns:
            pd.DataFrame: Budget data from the CSV file, or empty DataFrame if error occurs

        Raises:
            Exception: Catches and logs any CSV reading errors, returns empty DataFrame
        """
        try:
            df = pd.read_csv(csv_path)
            return df
        except Exception as e:
            print(f"Error reading budget from {csv_path}: {e}")
            return pd.DataFrame()
        
    # --- Main logic to iterate and combine CSVs ---
    all_budget_df = pd.DataFrame()
    csv_files_found = 0

    print(f"\n--- Bronze Layer: Starting CSV Data Ingestion from {DATA_DIRECTORY} ---")

    if not os.path.exists(DATA_DIRECTORY):
        print(f"Error: Data directory '{DATA_DIRECTORY}' not found. Please verify the path.")
    else:
        for filename in os.listdir(DATA_DIRECTORY):
            file_path = os.path.join(DATA_DIRECTORY, filename)
            
            # Process only files ending with '_budget.csv' and skip directories
            if os.path.isdir(file_path) or not filename.endswith("_budget.csv"):
                continue

            # Derive project_id from filename (e.g., "BE01" from "BE01_budget.csv")
            project_name_from_file = os.path.splitext(filename)[0]
            print(f"project_name_from_file: {project_name_from_file}")
            base_project_id = project_name_from_file.replace('_budget', '')

            # Get country from our PROJECT_CURRENCY_MAP
            project_info = PROJECT_CURRENCY_MAP.get(base_project_id)
            if not project_info:
                print(f"Warning: Project '{base_project_id}' (from '{filename}') not found in PROJECT_CURRENCY_MAP. Skipping.")
                continue
            
            country_from_map = project_info['country']

            print(f"  Ingesting budget from CSV: {filename} (Project: {base_project_id}, Country: {country_from_map})...")
            df = extract_budget_from_csv(file_path)
            # print(f"df: \n{df.head()}")
            
            if not df.empty:
                df['project_id'] = base_project_id
                df['country'] = country_from_map
                # Also add the original_currency_map for consistency, even if budget is already in EUR
                all_budget_df = pd.concat([all_budget_df, df], ignore_index=True)
                csv_files_found += 1
            else:
                print(f" No data extracted or error occurred for {filename}.")

    return all_budget_df

def bronze_expenses() -> pd.DataFrame:
    """
    Bronze layer function for ingesting expenses data from SQLite database files.

    This function implements the first stage of the medallion architecture by:
    - Connecting to all SQLite database files in the data directory
    - Extracting expenses data from the 'expenses' table in each database
    - Standardizing project IDs, countries, and currencies
    - Combining all expenses data into a single DataFrame
    - Adding metadata columns (project_id, country, original_currency)

    The function processes files matching the pattern '*.db' and applies
    a standardized project-to-country-currency mapping to ensure data consistency.

    Returns:
        pd.DataFrame: Combined expenses DataFrame containing:
            - All original expense columns from database tables
            - project_id (str): Standardized project identifier
            - country (str): Country name from PROJECT_CURRENCY_MAP
            - original_currency (str): Currency code from PROJECT_CURRENCY_MAP

    Raises:
        FileNotFoundError: If DATA_DIRECTORY does not exist
        sqlite3.Error: If database connection or query fails

    Example:
        >>> expenses_df = bronze_expenses()
        >>> print(expenses_df.columns)
        Index(['amount_local', 'amount_eur', 'year', 'month', 'department', 'category', 'currency', 'id', 'project_id', 'country', 'original_currency'])

    Note:
        - Only processes files ending with '.db'
        - Assumes 'expenses' table exists in each database
        - Skips projects not found in PROJECT_CURRENCY_MAP
        - Returns empty DataFrame if no valid files are found
        - Safely closes database connections even if errors occur
    """
    # --- Bronze Layer: Ingesting Expenses from SQLite DBs ---
    def extract_expenses_from_db(db_path: str) -> pd.DataFrame:
        """
        Extract expenses data from a single SQLite database file.

        Connects to the SQLite database and executes a SELECT query on the 'expenses' table.
        Handles database connection management and error handling.

        Args:
            db_path (str): Full path to the SQLite database file

        Returns:
            pd.DataFrame: Expenses data from the database, or empty DataFrame if error occurs

        Raises:
            sqlite3.Error: Database connection or query errors are caught and logged

        Note:
            - Assumes 'expenses' table exists in the database
            - Connection is safely closed in finally block
        """
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            query = "SELECT * FROM expenses" # Assumes 'expenses' is the table name
            df = pd.read_sql_query(query, conn)
            return df
        except sqlite3.Error as e:
            print(f"Error reading expenses from {db_path}: {e}")
            return pd.DataFrame() # Return empty DataFrame on error
        finally:
            if conn:
                conn.close()


    # --- Main logic to iterate and combine DBs ---
    all_expenses_df = pd.DataFrame()
    db_files_found = 0

    # Define project currency map for expenses
    PROJECT_CURRENCY_MAP = {
        "BE01": {"country": "Belgium", "currency": "EUR"},
        "BE55": {"country": "Belgium", "currency": "EUR"},
        "KE01": {"country": "Kenya", "currency": "KES"},
        "KE02": {"country": "Kenya", "currency": "KES"},   
        "SN01": {"country": "Senegal", "currency": "XOF"},
        "SN02": {"country": "Senegal", "currency": "XOF"},
        "BF01": {"country": "Burkina Faso", "currency": "XOF"},
        "BF02": {"country": "Burkina Faso", "currency": "XOF"},
    }

    print(f"\n--- Bronze Layer: Starting DB Data Ingestion from {DATA_DIRECTORY} ---")

    if not os.path.exists(DATA_DIRECTORY):
        print(f"Error: Data directory '{DATA_DIRECTORY}' not found. Please verify the path.")
    else:
        for filename in os.listdir(DATA_DIRECTORY):
            file_path = os.path.join(DATA_DIRECTORY, filename)
            
            # Process only files ending with '.db' and skip directories
            if os.path.isdir(file_path) or not filename.endswith(".db"):
                continue

            # Derive project_id from filename (e.g., "BE01" from "BE01.db")
            project_name_from_file = os.path.splitext(filename)[0]
            base_project_id = project_name_from_file # For DBs, the filename directly is the project_id

            # Get country and currency from our PROJECT_CURRENCY_MAP
            project_info = PROJECT_CURRENCY_MAP.get(base_project_id)
            if not project_info:
                print(f"Warning: Project '{base_project_id}' (from '{filename}') not found in PROJECT_CURRENCY_MAP. Skipping.")
                continue
            
            country_from_map = project_info['country']
            currency_from_map = project_info['currency'] # This will be our source of truth for currency

            print(f"  Ingesting expenses from DB: {filename} (Project: {base_project_id}, Country: {country_from_map})...")
            df = extract_expenses_from_db(file_path)
            
            if not df.empty:
                df['project_id'] = base_project_id
                df['country'] = country_from_map
                df['original_currency'] = currency_from_map # Add the true currency from our map
                all_expenses_df = pd.concat([all_expenses_df, df], ignore_index=True)
                db_files_found += 1
            else:
                print(f"   No data extracted or error occurred for {filename}.")

    return all_expenses_df