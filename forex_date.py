# import yfinance as yf
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import pytz
import requests

def get_forex_rate_at_datetime(pair, target_datetime, api_key):
    """
    Retrieves the forex exchange rate for a given currency pair at a specific date and time.

    Args:
        pair (str): The currency pair (e.g., "GBPEUR").
        target_datetime (str or datetime): The target date and time in 'YYYY-MM-DD HH:MM:SS' format or a datetime object.

    Returns:
        float: The exchange rate at the specified date and time, or None if not found or an error occurs.
    """
    try:
        # Validate the currency pair
        if not isinstance(pair, str):
            raise ValueError("Invalid currency pair format. It should be a string (e.g., 'GBPEUR').")

        # Validate and parse the target datetime
        if isinstance(target_datetime, str):
            try:
                target_datetime = datetime.strptime(target_datetime, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise ValueError("Incorrect datetime format, should be YYYY-MM-DD HH:MM:SS")
        elif not isinstance(target_datetime, datetime):
            raise ValueError("target_datetime must be a string or a datetime object")

        # Ensure the datetime is timezone-aware (UTC)
        if target_datetime.tzinfo is None or target_datetime.tzinfo.utcoffset(target_datetime) is None:
            target_datetime = pytz.utc.localize(target_datetime)
        else:
            target_datetime = target_datetime.astimezone(pytz.utc)

        # Check if the target datetime is a weekday or weekend
        if target_datetime.weekday() >= 5:  # 5 = Sat, 6 = Sun
            print("The target datetime falls on a weekend.")
            # Set lastfx_datetime to midnight of the Friday before
            lastfx_datetime = target_datetime - timedelta(days=target_datetime.weekday() - 4)
            lastfx_datetime = lastfx_datetime.replace(hour=23, minute=59, second=59, microsecond=0)
            print(f"Using last available forex datetime: {lastfx_datetime}")
            getfx_datetime = lastfx_datetime
        else:
            getfx_datetime = target_datetime
            
        # Download the data using yfinance
        # # Get the daily date and then find the closest time.
        # start_date = target_datetime.strftime('%Y-%m-%d')
        # end_date = (target_datetime + timedelta(days=1)).strftime('%Y-%m-%d')
        # data = yf.download(pair + '=X', start=start_date, end=end_date)

        # API key for FXmarketAPI
        # YzP2czMSdhFjGJo5hcI5
        # https://fxmarketapi.com/apipandas?currency=EURUSD,GBPUSD&start_date=2018-07-02&end_date=2018-09-03&interval=hourly&api_key=api_key
        # URL = "https://fxmarketapi.com/apipandas"
        # params = {'currency' : 'GBPEUR',
        # 'start_date' : start_date,
        # 'end_date': end_date,
        # 'interval': 'minute',
        # 'api_key':'YzP2czMSdhFjGJo5hcI5'}
        # response = requests.get(URL, params=params)
        # df = pd.read_json(response.text)
        # "https://fxmarketapi.com/apipandas?api_key=api_key&currency=EURUSD,GBPUSD&start_date=2018-07-02&end_date=2018-09-03")
        # df = pd.read_json("https://fxmarketapi.com/apipandas" + 
        #                   '?api_key=' + api_key + 
        #                   '&currency=' + pair + 
        #                   '&start_date=' + start_date + 
        #                   '&end_date=' + end_date + 
        #                   '&interval=' + 'minute')
        
        # https://fxmarketapi.com/apihistorical?currency=GBPEUR&date=2020-03-12-19:51&interval=minute&api_key=YzP2czMSdhFjGJo5hcI5
        fx_api_str = ("https://fxmarketapi.com/apihistorical" + 
                    '?' +
                    'currency=' + pair +
                    '&' +
                    'date=' + getfx_datetime.strftime('%Y-%m-%d-%H:%M') + 
                    '&' +
                    'interval=' + 'minute' + 
                    '&' +
                    'api_key=' + api_key)
        print(fx_api_str)
        response = requests.get(fx_api_str)
        print(response.text)
        df = pd.read_json(StringIO(response.text))

        if df.empty:
            print(f"No data found for {pair} on {target_datetime.strftime('%Y-%m-%d-%H:%M')}.")
            return None

        print(df)

        # Get the exchange rate
        exchange_rate = float(df.price.iloc[0])

        return exchange_rate

    except ValueError as e:
        print(f"Error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def get_historical_forex_data(pair, start_date, end_date):
    """
    Retrieves historical forex data for a given currency pair from Yahoo Finance.

    Args:
        pair (str): The currency pair (e.g., "GBPEUR=X").
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        pandas.DataFrame: A DataFrame containing the historical forex data, or None if an error occurs.
    """
    try:
        # Validate the currency pair
        if not isinstance(pair, str) or not pair.endswith("=X"):
            raise ValueError("Invalid currency pair format. It should be a string ending with '=X' (e.g., 'GBPEUR=X').")

        # Validate date formats
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Incorrect date format, should be YYYY-MM-DD")

        # Download the data using yfinance
        data = yf.download(pair, start=start_date, end=end_date)

        if data.empty:
            print(f"No data found for {pair} between {start_date} and {end_date}.")
            return None

        # Rename the 'Adj Close' column to 'Close' for consistency
        if 'Adj Close' in data.columns:
            data.rename(columns={'Adj Close': 'Close'}, inplace=True)

        return data

    except ValueError as e:
        print(f"Error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None



def test_forex_timepoint():
    """
    Example usage of the get_forex_rate_at_datetime function.
    """
    currency_pair = "GBPEUR"  # GBP/EUR currency pair
    target_datetime_str = "2020-12-19 15:18:00"  # Example date and time
    target_datetime_obj = datetime(2023, 7, 15, 12, 0, 0)
    with open('.fx_api_key', 'r') as key_file:
        api_key = key_file.read().strip()

    # Example usage with string
    exchange_rate = get_forex_rate_at_datetime(currency_pair, target_datetime_str, api_key)

    # Example usage with datetime object
    # exchange_rate = get_forex_rate_at_datetime(currency_pair, target_datetime_obj)

    if exchange_rate is not None:
        print(f"Exchange rate: {exchange_rate:.4f}")


def test_forex_timerange():
    """
    Example usage of the get_historical_forex_data function.
    """
    currency_pair = "GBPEUR=X"  # GBP/EUR currency pair
    start_date = "2023-01-01"
    end_date = "2023-12-31"

    forex_data = get_historical_forex_data(currency_pair, start_date, end_date)

    if forex_data is not None:
        print(f"Historical Forex Data for {currency_pair} ({start_date} to {end_date}):")
        print(forex_data)
        # You can save the data to a CSV file if needed
        # forex_data.to_csv(f"{currency_pair.replace('=X','')}_{start_date}_{end_date}.csv")
        # print(f"Data saved to {currency_pair.replace('=X','')}_{start_date}_{end_date}.csv")

        # Example: Calculate the average closing price
        average_closing_price = forex_data['Close'].mean()
        print(f"\nAverage Closing Price: {average_closing_price:.4f}")

        # Example: Find the highest and lowest closing prices
        highest_closing_price = forex_data['Close'].max()
        lowest_closing_price = forex_data['Close'].min()
        print(f"Highest Closing Price: {highest_closing_price:.4f}")
        print(f"Lowest Closing Price: {lowest_closing_price:.4f}")

        # Example: Get the closing price on a specific date
        specific_date = "2023-07-15"
        try:
            closing_price_on_date = forex_data.loc[specific_date, 'Close']
            print(f"Closing Price on {specific_date}: {closing_price_on_date:.4f}")
        except KeyError:
            print(f"No data found for {specific_date}")
        
        # Example: Get the last closing price
        last_closing_price = forex_data['Close'].iloc[-1]
        print(f"Last Closing Price: {last_closing_price:.4f}")

if __name__ == "__main__":
    test_forex_timepoint()
