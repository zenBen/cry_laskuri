import pandas as pd
import sys
import os
import csv

def ledger_to_trades(ledger_file, trades_file, output_file):
    """
    Converts ledger entries (buy/sell) to a trades-like format matching the structure of trades.csv.
    ['deposit', 'trade', 'transfer', 'withdrawal', 
    'staking', 'spend', 'receive', 'earn']

    Args:
        ledger_file (str): Path to the ledger CSV file.
        trades_file (str): Path to the trades CSV file.
        output_file (str): Path to the output trades CSV file.
    """

    coins = ['BTC', 'LTC', 'XLM', 'XMR', 'ETH', 'ETC', 'REP', 'XRP',
            'ZEC', 'BCH', 'BSV', 'SGB', 'FLR', 'STRK', 'EIGEN']

    try:
        # Read the ledger CSV into a pandas DataFrame
        df_ledger = pd.read_csv(ledger_file)
    except FileNotFoundError:
        print(f"Error: Ledger file not found at {ledger_file}")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Ledger file is empty at {ledger_file}")
        return
    except Exception as e:
        print(f"An unexpected error occurred while reading the ledger file: {e}")
        return

    try:
        # Read the trades CSV into a pandas DataFrame
        df_trades_csv = pd.read_csv(trades_file)
    except FileNotFoundError:
        print(f"Error: Trades file not found at {trades_file}")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Trades file is empty at {trades_file}")
        return
    except Exception as e:
        print(f"An unexpected error occurred while reading the trades file: {e}")
        return

    # Filter out non-trade entries
    df_trades = df_ledger[df_ledger["type"] == "trade"].copy()
    if df_trades.empty:
        print(f"No 'trade' type entries found in {ledger_file}")
        return

    # Group trades by refid
    grouped_trades = df_trades.groupby("refid")

    # Prepare data for output
    output_data = []

    for refid, group in grouped_trades:
        if len(group) != 2:
            print(f"Warning: refid {refid} has {len(group)} rows, expected 2. Skipping.")
            continue

        # Determine buy/sell type
        fiat_row = None
        crypto_row = None
        for index, row in group.iterrows():
            if row['asset'] in ['EUR', 'GBP']:  # Basic fiat check
                fiat_row = row
            elif row["asset"] in coins:
                crypto_row = row

        if fiat_row is None or crypto_row is None:
            print(f"Error: refid {refid} does not have one fiat and one crypto row. Skipping.")
            continue

        if fiat_row['amount'] < 0:
            trade_type = "buy"
        elif fiat_row['amount'] > 0:
            trade_type = "sell"
        else:
            print(f"Error: refid {refid} fiat amount not positive or negative. Skipping.")
            continue

        # Construct ledgers field
        ledgers = f"{group['txid'].iloc[1]},{group['txid'].iloc[0]}"

        # Basic calculation of price (needs adjustment for real scenarios)
        price = abs(fiat_row['amount']) / abs(crypto_row['amount']) if crypto_row['amount'] != 0 else 0

        # Find matching ordertxid from trades.csv
        matching_trade = df_trades_csv[df_trades_csv['txid'] == refid]
        ordertxid = matching_trade['ordertxid'].iloc[0] if not matching_trade.empty else ""

        # Construct output row
        output_row = {
            "txid": refid,  # txid now takes the value of refid
            "ordertxid": ordertxid, # copy from trades.csv if available
            "pair": f"{crypto_row['asset']}/{fiat_row['asset']}", # Simple asset pairing
            "aclass": "forex" if fiat_row['asset'] in ['EUR', 'GBP'] else "currency", # aclass updated
            "time": group['time'].iloc[0],  # Using first time as an approximation
            "type": trade_type,
            "ordertype": "", # Could be derived based on available info, but we have no order book
            "price": price,
            "cost": abs(fiat_row['amount']),
            "fee": fiat_row['fee'] + crypto_row['fee'],
            "vol": abs(crypto_row['amount']),
            "margin": "",
            "misc": "",
            "ledgers": ledgers,
            "posttxid": "",
            "posstatuscode": "",
            "cprice": "",
            "ccost": "",
            "cfee": "",
            "cvol": "",
            "cmargin": "",
            "net": "",
            "trades": "",
        }
        output_data.append(output_row)

    # Process spend/receive pairs where both assets are cryptocoins
    df_spend_receive = df_ledger[df_ledger["type"].isin(["spend", "receive"])].copy()
    grouped_spend_receive = df_spend_receive.groupby("refid")

    for refid, group in grouped_spend_receive:
        if len(group) != 2:
            print(f"Warning: refid {refid} has {len(group)} rows, expected 2. Skipping.")
            continue

        spend_row = None
        receive_row = None
        for index, row in group.iterrows():
            if row["type"] == "spend" and row["asset"] in coins:
                spend_row = row
            elif row["type"] == "receive" and row["asset"] in coins:
                receive_row = row

        if spend_row is None or receive_row is None:
            print(f"Error: refid {refid} does not have one spend and one receive row with cryptocoins. Skipping.")
            continue

        # Construct ledgers field
        ledgers = f"{group['txid'].iloc[1]},{group['txid'].iloc[0]}"

        # Basic calculation of price (needs adjustment for real scenarios)
        price = abs(spend_row['amount']) / abs(receive_row['amount']) if receive_row['amount'] != 0 else 0

        # Find matching ordertxid from trades.csv
        matching_trade = df_trades_csv[df_trades_csv['txid'] == refid]
        ordertxid = matching_trade['ordertxid'].iloc[0] if not matching_trade.empty else ""

        # Construct output row
        output_row = {
            "txid": refid,  # txid now takes the value of refid
            "ordertxid": ordertxid, # copy from trades.csv if available
            "pair": f"{spend_row['asset']}/{receive_row['asset']}", # Simple asset pairing
            "aclass": "currency", # aclass updated
            "time": group['time'].iloc[0],  # Using first time as an approximation
            "type": "trade",
            "ordertype": "", # Could be derived based on available info, but we have no order book
            "price": price,
            "cost": abs(spend_row['amount']),
            "fee": spend_row['fee'] + receive_row['fee'],
            "vol": abs(receive_row['amount']),
            "margin": "",
            "misc": "",
            "ledgers": ledgers,
            "posttxid": "",
            "posstatuscode": "",
            "cprice": "",
            "ccost": "",
            "cfee": "",
            "cvol": "",
            "cmargin": "",
            "net": "",
            "trades": "",
        }
        output_data.append(output_row)

    # Write to output CSV
    if output_data:
        df_output = pd.DataFrame(output_data)
        # order rows by time
        df_output['time'] = pd.to_datetime(df_output['time'])
        df_output = df_output.sort_values(by='time')
        df_output['time'] = df_output['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

        df_output.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
        print(f"Successfully converted ledger data to trades data. Output saved to {output_file}")
    else:
        print("No valid trades to output.")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        in_path = sys.argv[1]
    else:
        in_path = os.getcwd()
    
    ledger_file = os.path.join(in_path, "ledgers.csv")
    trades_file = os.path.join(in_path, "trades.csv")

    if len(sys.argv) == 3:
        output_file = sys.argv[2]
    else:
        output_file = os.path.join(os.getcwd(), "trades_generated.csv")
    ledger_to_trades(ledger_file, trades_file, output_file)