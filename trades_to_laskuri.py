import pandas as pd
import sys
import os

def process_trades_for_laskuri(coin, file_path):
    """
    Processes trade data from a CSV file, filters it by a specific coin,
    and converts it to a format suitable for Laskuri tax reporting.

    Args:
        coin (str): The cryptocurrency to filter by (e.g., 'BTC', 'LTC', 'XMR').
        file_path (str): The path to the trades CSV file.

    Returns:
        str: The path to the processed CSV file.
    """
    try:
        # Load the dataset
        dataset = pd.read_csv(file_path)

        # Filter for rows where 'pair' includes the specified coin
        coin_data = dataset[dataset['pair'].str.contains(coin, na=False)]

        # Convert to the required format
        converted_data = pd.DataFrame({
            'AIKA - DATE/TIME': pd.to_datetime(coin_data['time']).dt.strftime('%d.%m.%Y %H:%M'),
            'TAPAHTUMA - EVENT': coin_data['type'].map({'buy': 'Osto', 'sell': 'Myynti'}),
            'MÄÄRÄ - AMOUNT': coin_data['vol'],
            'HINTA € / VIRTUAALIVALUUTTA - PRICE PER UNIT': coin_data['price'],
            'YHTEENSÄ - TOTAL': coin_data['cost'],
            'fee': coin_data['fee'],
            'LÄHDE - SOURCE': 'Kraken',
            'VIRTUAALIVALUUTTAA JÄLJELLÄ 1 - CURRENCY REMAINING 1': coin_data['vol'],
            'HANKINTAMENO TAI HANKINTAMENO-OLETTAMA/KULUTETTU VIRTUAALIVALUUTTA - PURCHASE COST OR DEEMED ACQ. COST': '',
            'DEEMED ACQ COST': '',
            'VOITTO /TAPPIO - PROFIT / LOSS': '',
            'VIRTUAALIVALUUTTAA JÄLJELLÄ 2 - CURRENCY REMAINING 2': '',
        })

        # Step 1: Convert decimals from period to comma
        converted_data['MÄÄRÄ - AMOUNT'] = converted_data['MÄÄRÄ - AMOUNT'].map(lambda x: f"{x:.8f}".replace('.', ','))
        converted_data['HINTA € / VIRTUAALIVALUUTTA - PRICE PER UNIT'] = converted_data['HINTA € / VIRTUAALIVALUUTTA - PRICE PER UNIT'].map(lambda x: f"{x:.2f} €".replace('.', ','))
        converted_data['YHTEENSÄ - TOTAL'] = converted_data['YHTEENSÄ - TOTAL'].map(lambda x: f"{x:.2f} €".replace('.', ','))

        # Convert AMOUNT back to numeric for calculations
        converted_data['amount_numeric'] = converted_data['MÄÄRÄ - AMOUNT'].str.replace(',', '.').astype(float)

        # Step 2: Calculate CURRENCY REMAINING correctly
        currency_remaining = 0
        currency_remaining_list = []

        for idx, row in converted_data.iterrows():
            event = row['TAPAHTUMA - EVENT']
            amount = row['amount_numeric']

            if event == 'Osto':
                currency_remaining += amount
            elif event == 'Myynti':
                currency_remaining -= amount

            currency_remaining_list.append(currency_remaining)

        converted_data['currency_remaining'] = currency_remaining_list

        # Step 3, 4 & 5: Add DEEMED ACQ COST explicitly, fix Purchase Cost and Profit/Loss
        purchase_queue = []

        for idx, row in converted_data.iterrows():
            event = row['TAPAHTUMA - EVENT']
            amount = row['amount_numeric']
            price_per_unit = float(row['HINTA € / VIRTUAALIVALUUTTA - PRICE PER UNIT'].replace(',', '.').replace(' €', ''))
            total = float(row['YHTEENSÄ - TOTAL'].replace(',', '.').replace(' €', ''))

            if event == 'Osto':
                purchase_queue.append({'remaining': amount, 'price': price_per_unit})
                converted_data.at[idx, 'HANKINTAMENO TAI HANKINTAMENO-OLETTAMA/KULUTETTU VIRTUAALIVALUUTTA - PURCHASE COST OR DEEMED ACQ. COST'] = ''
                converted_data.at[idx, 'DEEMED ACQ COST'] = ''
                converted_data.at[idx, 'VOITTO /TAPPIO - PROFIT / LOSS'] = ''
            elif event == 'Myynti':
                purchase_cost = 0
                remaining_amount = amount

                # obtain the sales fee
                fee = row['fee']

                # Apply FIFO purchase cost calculation
                while remaining_amount > 0 and purchase_queue:
                    purchase = purchase_queue[0]
                    used_amount = min(remaining_amount, purchase['remaining'])
                    purchase_cost += used_amount * purchase['price']
                    purchase['remaining'] -= used_amount
                    remaining_amount -= used_amount

                    if purchase['remaining'] <= 0:
                        purchase_queue.pop(0)

                # add fee to purchase cost becos fees aren't deductible from deemed acq cost
                cost_plus_fee = purchase_cost + fee

                deemed_acq_cost = price_per_unit * 0.2 * amount  # 20% deemed acquisition cost
                applicable_cost = max(cost_plus_fee, deemed_acq_cost)
                profit_or_loss = total - applicable_cost

                # Format values for concatenation
                purchase_cost_str = f"{purchase_cost:.2f} €".replace('.', ',')
                deemed_acq_cost_str = f"{deemed_acq_cost:.2f} €".replace('.', ',')

                # Parentheses logic and concatenation
                if cost_plus_fee < deemed_acq_cost:
                    combined_str = f"({purchase_cost_str}) / {deemed_acq_cost_str}"
                else:
                    combined_str = f"{purchase_cost_str} / ({deemed_acq_cost_str})"

                # Overwrite, then drop extra
                converted_data.at[idx, 'HANKINTAMENO TAI HANKINTAMENO-OLETTAMA/KULUTETTU VIRTUAALIVALUUTTA - PURCHASE COST OR DEEMED ACQ. COST'] = combined_str

                # Convert calculated values to strings with comma as decimal separator
                converted_data.at[idx, 'VOITTO /TAPPIO - PROFIT / LOSS'] = f"{profit_or_loss:.2f} €".replace('.', ',')

        # Format CURRENCY REMAINING fields
        converted_data['VIRTUAALIVALUUTTAA JÄLJELLÄ 1 - CURRENCY REMAINING 1'] = converted_data['currency_remaining'].map(lambda x: f"{x:.8f}".replace('.', ','))
        converted_data['VIRTUAALIVALUUTTAA JÄLJELLÄ 2 - CURRENCY REMAINING 2'] = converted_data['currency_remaining'].map(lambda x: f"{x:.8f}".replace('.', ','))

        # Drop helper columns
        converted_data.drop(columns=['amount_numeric', 'currency_remaining', 'DEEMED ACQ COST'], inplace=True)

        # Save the final processed file
        processed_file_name = f'processed_trades_{coin}.csv'
        processed_file_path = os.path.join(os.path.dirname(file_path), processed_file_name) # saves to the same dir as the input file
        converted_data.to_csv(processed_file_path, index=False)

        return processed_file_path
    except FileNotFoundError:
        print(f"Error: File not found at path: {file_path}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python trades_to_laskuri.py <coin> [<file_path>]")
        print("  <coin>: The cryptocurrency to filter by (e.g., BTC, LTC, XMR).")
        print("  <file_path>: (Optional) The path to the trades CSV file. Defaults to current directory.")
        sys.exit(1)

    coin = sys.argv[1]
    
    if len(sys.argv) == 3:
        file_path = sys.argv[2]
    else:
        file_path = os.path.join(os.getcwd(), "trades.csv")  # Default to current directory/trades.csv
    

    result_path = process_trades_for_laskuri(coin, file_path)

    if result_path:
        print(f"Processed data saved to: {result_path}")
