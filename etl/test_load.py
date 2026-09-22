import os
import pandas as pd

file_path = r"C:\FactoryReporting\DrumReport.xlsx"

print("Checking file:")
print(file_path)
print("Exists:", os.path.exists(file_path))

df = pd.read_excel(file_path)

print("Rows:", len(df))
print("Columns:", list(df.columns))
print(df.head())
