# MSF Data Pipeline - Medallion Architecture Implementation

## Overview

This project implements a **Medallion Architecture** data pipeline for processing MSF (Médecins Sans Frontières) budget and expenses data. The pipeline follows the Bronze-Silver-Gold layered approach to transform raw data into analysis-ready datasets for Power BI modeling.

## Architecture

### Medallion Architecture Layers

The project is structured using the medallion architecture pattern with three distinct layers:

#### 🥉 Bronze Layer (`bronze_layer.py`)
The **ingestion stage** of our data pipeline that handles raw data extraction:

- **Budget Data Ingestion**:
  - Ingests budget data from CSV files (`*_budget.csv`)
  - Combines multiple CSV files into one large DataFrame
  - Handles project mapping and country/currency standardization

- **Expenses Data Ingestion**:
  - Ingests expenses data from SQLite databases (`*.db`)
  - Extracts data from the `expenses` table in each database
  - Combines all expense records into one large DataFrame
  - Applies project-specific country and currency corrections

#### 🥈 Silver Layer (`silver_layer.py`)
The **data cleaning and transformation stage**:

- **Data Quality Improvements**:
  - Removes unnecessary columns (`id`, `version`, `currency`)
  - Standardizes project IDs (e.g., KEO2 → KE02)
  - Creates proper date columns from year/month data
  - Converts dates to month-end format for consistency

- **Currency Conversion**:
  - Fetches latest exchange rates using external API
  - Adds exchange rate columns for currency conversion
  - Calculates EUR equivalent amounts for expenses

#### 🥇 Gold Layer (`gold_layer.py`)
The **business-ready data stage**:

- **Data Integration**:
  - Merges cleaned budget and expenses DataFrames
  - Performs inner join on common dimensions: `date`, `project_id`, `country`, `department`, `category`
  - Creates the final analytical dataset

## Project Structure

```
MSF-test2/
├── main.py                    # Main execution script
├── bronze_layer.py           # Bronze layer implementation
├── silver_layer.py           # Silver layer implementation
├── gold_layer.py             # Gold layer implementation
├── get_latest_exchange_rate.py # Exchange rate API utility
├── notebook.ipynb           # Development notebook (test arena)
├── test-notebook.ipynb      # Additional testing notebook
├── processed_data/          # Output directory
│   └── gold_df.csv         # Final processed dataset
├── OneDrive_1_7-4-2025/    # Source data directory
│   ├── *.csv               # Budget CSV files
│   ├── *.db                # Expense database files
│   └── *.png               # Power BI screenshots
└── README.md               # This file
```

## Data Sources

### Countries and Projects Covered:
- **Belgium**: BE01, BE55 (EUR)
- **Kenya**: KE01, KE02 (KES)
- **Senegal**: SN01, SN02 (XOF)
- **Burkina Faso**: BF01, BF02 (XOF)

### Data Files:
- **Budget Data**: CSV files containing budget allocations by project
- **Expenses Data**: SQLite databases containing actual expense records

## Usage

### Running the Pipeline

Execute the main pipeline:

```bash
python main.py
```

This will:
1. Process data through all three medallion layers
2. Generate the final gold dataset
3. Save the result to `processed_data/gold_df.csv`

### Development and Testing

The notebook files (`notebook.ipynb`, `test-notebook.ipynb`) were used as a **test arena** for rough work and development of the main source files. They contain experimental code and data exploration that informed the final implementation.

## Exchange Rate Integration

### API Configuration
- **API Provider**: ExchangeRate-API
- **Get Your Free API Key**: https://app.exchangerate-api.com
- **Configuration**: API key is stored securely in `.env` file
- **Endpoint**: Latest exchange rates only

### Environment Setup
Create a `.env` file in the project root with your API key:
```
EXCHANGE_RATE_API_KEY=your_api_key_here
```

⚠️ **Security Note**: The `.env` file contains sensitive information and should never be committed to version control. Make sure it's included in your `.gitignore` file.

### Important Limitation
⚠️ **Note**: Due to financial constraints, we used the free API tier which only provides **latest exchange rates**. We could not access historical exchange rate data to find exact rates for specific past dates. Therefore, the `rate` column uses current exchange rates for all historical expense conversions.

This limitation affects the accuracy of EUR conversions for historical expenses, as they use today's exchange rates rather than the rates that were applicable at the time of the original transactions.

## Output

The final gold DataFrame is saved in `processed_data/gold_df.csv` and contains:
- Merged budget and expense data
- Standardized date formats
- Currency conversions to EUR
- Clean, analysis-ready structure for Power BI modeling

## Power BI Integration

The processed dataset is designed for seamless integration with Power BI for:
- Financial analysis and reporting
- Budget vs. actual expense comparisons
- Multi-country project performance tracking
- Currency-normalized financial insights

## Technical Dependencies

- `pandas`: Data manipulation and analysis
- `sqlite3`: Database connectivity
- `requests`: API calls for exchange rates
- `numpy`: Numerical operations
- `python-dotenv`: Environment variable management

### Installation
Install required packages:
```bash
pip install -r requirements.txt
```

---

**Copyright © Haji Rufai 2025**

*This implementation demonstrates a production-ready medallion architecture for financial data processing, with proper separation of concerns across bronze, silver, and gold layers.*