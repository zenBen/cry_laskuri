import pandas as pd
import argparse

def convert_ledger_to_trades(ledger_filepath, output_filepath):
    """
    Converts ledger entries (buy/sell) to a trades-like format matching the structure of trades.csv.

    Args:
        ledger_filepath (str): Path to the ledger CSV file.
        output_filepath (str): Path to save the converted trades CSV file.
    """

    try:
        # Load the ledger data
        ledger_df = pd.read_csv(ledger_filepath)
    except FileNotFoundError:
        print(f"Error: Ledger file not found at {ledger_filepath}")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: Ledger file is empty at {ledger_filepath}")
        return
    except pd.errors.ParserError:
        print(f"Error: Could not parse ledger file at {ledger_filepath}")
        return

    # Filter for 'trade' type entries
    trade_entries = ledger_df[ledger_df['type'] == 'trade'].copy()

    if trade_entries.empty:
        print("No trade entries found in the ledger.")
        return

    # Rename and select necessary columns
    trade_entries.rename(columns={
        'txid': 'ledgers',
        'refid': 'ordertxid',
        'time': 'time',
        'amount': 'vol',
        'fee': 'fee',
        'asset':'asset'
    }, inplace=True)

    trade_entries['time'] = pd.to_datetime(trade_entries['time'])

    # Initialize new columns with default values. These columns are in the trades.csv
    trade_entries['txid'] = ""
    trade_entries['pair'] = ""
    trade_entries['aclass'] = "forex"  # Assuming forex for all
    trade_entries['type'] = ""
    trade_entries['ordertype'] = ""
    trade_entries['price'] = 0.0
    trade_entries['cost'] = 0.0
    trade_entries['margin'] = 0.0
    trade_entries['misc'] = ""
    trade_entries['posttxid'] = ""
    trade_entries['posstatuscode'] = ""
    trade_entries['cprice'] = ""
    trade_entries['ccost'] = ""
    trade_entries['cfee'] = ""
    trade_entries['cvol'] = ""
    trade_entries['cmargin'] = ""
    trade_entries['net'] = ""
    trade_entries['trades'] = ""

    # Reorder columns to match trades.csv format
    # Keep the 'asset' column
    trade_entries = trade_entries[[
        'txid', 'ordertxid', 'pair', 'aclass', 'time', 'type', 'ordertype', 'price',
        'cost', 'fee', 'vol', 'margin', 'misc', 'ledgers', 'posttxid', 'asset',
        'posstatuscode', 'cprice', 'ccost', 'cfee', 'cvol', 'cmargin', 'net', 'trades'
    ]]

    # Process trades to determine type, price and cost
    # For each unique order ID
    for order_id in trade_entries['ordertxid'].unique():
        order_trades = trade_entries[trade_entries['ordertxid'] == order_id]
        if len(order_trades) == 2:  # Buy/sell pairs
            buy_trade = order_trades[order_trades['vol'] > 0].iloc[0]
            sell_trade = order_trades[order_trades['vol'] < 0].iloc[0]
            # If sell and buy are swapped, this will fail with an index out of bounds error.
            trade_entries.loc[buy_trade.name, 'type'] = 'buy'
            trade_entries.loc[sell_trade.name, 'type'] = 'sell'
            trade_entries.loc[buy_trade.name, 'vol'] = buy_trade['vol']
            trade_entries.loc[sell_trade.name, 'vol'] = -sell_trade['vol']
            
            # Populate the pair column
            trade_entries.loc[buy_trade.name, 'pair'] = f"{buy_trade['asset']}/{sell_trade['asset']}" if buy_trade['asset']!="EUR" else f"{sell_trade['asset']}/{buy_trade['asset']}"
            trade_entries.loc[sell_trade.name, 'pair'] = trade_entries.loc[buy_trade.name, 'pair']

            # Calculate price, cost, and type for the buy and sell legs
            buy_volume = buy_trade['vol']
            sell_volume = -sell_trade['vol']
            
            if buy_trade['asset'] != "EUR":
                cost = -sell_trade['vol']
                price = cost / buy_volume if buy_volume != 0 else 0
                trade_entries.loc[buy_trade.name, 'cost'] = cost
                trade_entries.loc[buy_trade.name, 'price'] = price

                trade_entries.loc[sell_trade.name, 'cost'] = -sell_trade['vol']
                trade_entries.loc[sell_trade.name, 'price'] = price
            else:
              cost = buy_trade['vol']
              price = cost / sell_volume if sell_volume != 0 else 0
              trade_entries.loc[buy_trade.name, 'cost'] = cost
              trade_entries.loc[buy_trade.name, 'price'] = price

              trade_entries.loc[sell_trade.name, 'cost'] = -buy_trade['vol']
              trade_entries.loc[sell_trade.name, 'price'] = price
        else:
            print(f"Warning: trade with order_id {order_id} could not be processed, wrong number of entries {len(order_trades)}.")

    # Fill other columns
    trade_entries['ordertype'] = "limit"
    trade_entries['txid'] = trade_entries['ordertxid']

    # Save the converted trades to a new CSV
    try:
        trade_entries.to_csv(output_filepath, index=False)
        print(f"Successfully converted ledger trades to {output_filepath}")
    except Exception as e:
        print(f"An error occurred while saving the output file: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert ledger entries to trades format.")
    parser.add_argument("ledger_filepath", help="Path to the ledger CSV file.")
    parser.add_argument("output_filepath", help="Path to save the output trades CSV file.")
    args = parser.parse_args()

    convert_ledger_to_trades(args.ledger_filepath, args.output_filepath)
