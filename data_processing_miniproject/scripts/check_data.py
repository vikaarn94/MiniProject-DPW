from pathlib import Path
import pandas as pd

DATA_PATH = Path("data/orders.csv")

df = pd.read_csv(DATA_PATH)

print("=== PEMERIKSAAN DATASET ===")
print("Lokasi file :", DATA_PATH)
print("Jumlah baris:", len(df))
print("Jumlah kolom:", len(df.columns))

print("\nNama kolom:")
print(df.columns.tolist())

print("\nLima data pertama:")
print(df.head())

print("\nJumlah nilai kosong per kolom:")
print(df.isnull().sum())

print("\nDistribusi status pesanan:")
print(df["status"].value_counts(dropna=False))