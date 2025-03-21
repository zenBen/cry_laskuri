import pandas as pd

# Load the CSV file
file_path = "/Users/bcowley/Benslab/tools/crypto/data/ledgers_crypto_2017-2023.csv"
output_path = file_path

# Read the CSV file into a DataFrame
df = pd.read_csv(file_path)

# Filter out rows where "type" is "withdrawal" and "asset" is a fiat currency
fiat_currencies = ["EUR", "GBP"]  # Add more fiat currencies if needed
filtered_df = df[~((df["type"] == "withdrawal") & (df["asset"].isin(fiat_currencies)))]

# Save the cleaned DataFrame to a new CSV file
filtered_df.to_csv(output_path, index=False)

print(f"Filtered data saved to {output_path}")