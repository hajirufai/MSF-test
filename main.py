import pandas as pd
from gold_layer import gold

# Accessing the gold_df from the gold layer (Medallion Architecture)
df = gold()

# saving the gold_df to a csv file
path = "/Users/hajirufai/test/MSF-test/processed_data/"  # use your actual path
df.to_csv(path + "gold_df.csv", index=False)

#Fact & Dim Table Modeling in Python
# Load your final Gold layer data
# df = pd.read_csv(path + 'gold_df.csv')

# Convert date column to datetime if it's not already
df['date'] = pd.to_datetime(df['date'])

# Create base date dimension
dim_date = df[['date']].drop_duplicates().reset_index(drop=True)

# Enrich with fields
dim_date['date_id'] = dim_date['date'].dt.strftime('%Y%m%d').astype(int)  # primary key
dim_date['year'] = dim_date['date'].dt.year
dim_date['month'] = dim_date['date'].dt.month
dim_date['day'] = dim_date['date'].dt.day
dim_date['year_id'] = dim_date['year']
dim_date['month_id'] = dim_date['date'].dt.strftime('%Y%m').astype(int)
dim_date['month_name'] = dim_date['date'].dt.strftime('%B')  # e.g., January
dim_date['day_name'] = dim_date['date'].dt.strftime('%A')    # e.g., Monday
dim_date['quarter'] = dim_date['date'].dt.quarter
dim_date['is_weekend'] = dim_date['day_name'].isin(['Saturday', 'Sunday'])

# reorder columns
dim_date = dim_date[[
    'date_id', 'date', 'year', 'month', 'day',
    'year_id', 'month_id', 'month_name', 'day_name',
    'quarter', 'is_weekend'
]]

dim_department = df[['department']].drop_duplicates().reset_index(drop=True)
dim_department['department_id'] = dim_department.index + 1

dim_country = df[['country']].drop_duplicates().reset_index(drop=True)
dim_country['country_id'] = dim_country.index + 1

dim_category = df[['category']].drop_duplicates().reset_index(drop=True)
dim_category['category_id'] = dim_category.index + 1

dim_project = df[['project_id']].drop_duplicates().reset_index(drop=True)
dim_project['project_id_numeric'] = dim_project.index + 1

# Merge keys back into the main dataframe
df = df.merge(dim_date[['date', 'date_id']], on='date', how='left') \
       .merge(dim_department, on='department', how='left') \
       .merge(dim_country, on='country', how='left') \
       .merge(dim_category, on='category', how='left') \
       .merge(dim_project, on='project_id', how='left')

# Fact Table
fact_expenses = df[['date_id', 'department_id', 'country_id', 'category_id', 'project_id_numeric', 'budget_eur', 'amount_eur']]

# Save to CSVs in processed_data directory
dim_date.to_csv(path + 'dim_date.csv', index=False)
dim_department.to_csv(path + 'dim_department.csv', index=False)
dim_country.to_csv(path + 'dim_country.csv', index=False)
dim_category.to_csv(path + 'dim_category.csv', index=False)
dim_project.to_csv(path + 'dim_project.csv', index=False)
fact_expenses.to_csv(path + 'fact_expenses.csv', index=False)

