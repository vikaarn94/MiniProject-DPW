import pandas as pd

path = "data/raw/orders.parquet"

df = pd.read_parquet(path)

print("=== HASIL PARQUET ===")
print("Jumlah record:", len(df))
print(df.head())
print("\nTipe data:")
print(df.dtypes)