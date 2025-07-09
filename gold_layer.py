import numpy as np
import pandas as pd
import os
from silver_layer import silver_budget, silver_expenses

def gold():
    """Returns the gold dataframe from the silver dataframes"""
    silver_budget_df = silver_budget()
    silver_expenses_df = silver_expenses()

    # Merge the silver_budget_df and silver_expenses_df
    common_columns = ['date', 'project_id', 'country', 'department', 'category']

    merged_df = pd.merge(silver_budget_df, silver_expenses_df, on=common_columns, how='inner')  # inner is default
    return merged_df
