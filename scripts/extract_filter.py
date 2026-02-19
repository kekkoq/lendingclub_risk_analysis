import pandas as pd
import sqlalchemy
import urllib
import os

print("--- Script Started ---", flush=True)

file_path = 'data/accepted_2007_to_2018Q4.csv.gz'
if not os.path.exists(file_path):
    print(f"ERROR: Could not find {file_path}. Did you download it yet?")
else:
    print(f"Found {file_path}. Starting chunked processing...")
    
    # Setup your columns and iterator
    cols = ['issue_d', 'loan_status', 'loan_amnt', 'int_rate', 'grade', 'purpose', 'annual_inc', 'dti', 'addr_state']
    iter_csv = pd.read_csv(file_path, usecols=cols, chunksize=50000)

    # Process and Filter
    chunks_found = []
    for i, chunk in enumerate(iter_csv):
        filtered_chunk = chunk[chunk['issue_d'].str.contains('2016|2017|2018', na=False)]
        chunks_found.append(filtered_chunk)
        
        if i % 10 == 0:
            print(f"Processed {i * 50000} rows...")

   # Combine all filtered chunks
    filtered_df = pd.concat(chunks_found)

# SQL Server Details 
server_name = r'KeikoPC\SQLEXPRESS'
database_name = 'LendingClubDB'
table_name = 'Filtered_Loans'

# Create the Connection String
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server_name};"
    f"DATABASE={database_name};"
    f"Trusted_Connection=yes;"
    f"TrustServerCertificate=yes;"
)

# Rename columns for SQL Server
rename_map = {
    'issue_d': 'Issue Date',
    'loan_status': 'Loan Status',
    'loan_amnt': 'Loan Amount',
    'int_rate': 'Interest Rate',
    'grade': 'Grade',
    'purpose': 'Purpose',
    'annual_inc': 'Annual Income',
    'dti': 'Debt-to-Income Ratio',
    'addr_state': 'State'
}

# Apply the rename
filtered_df.rename(columns=rename_map, inplace=True)

# Create the SQLAlchemy engine
engine = sqlalchemy.create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True)

# Push the data to SQL
print(f"Loading {len(filtered_df)} rows to SQL Server... this may take a few minutes.")
filtered_df.to_sql(
    table_name, 
    engine, 
    if_exists='replace', 
    index=False,
    chunksize=50000
)

print("SUCCESS! Your data is now in SQL Server.")

