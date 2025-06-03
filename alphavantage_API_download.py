import requests
import json
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError

from dotenv import load_dotenv
import os

load_dotenv()

db_params = {
    "host": os.getenv('DB_HOST'),
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
}

access_token = os.getenv('API_KEY_ALPHAVANTAGE')

EXPORT_PATH = "/Users/ilya/Desktop/AUXO_python_integration/data"
FILENAME = "alphavantage_{symbol}.json"

columns = ['1. open', '2. high', '3. low', '4. close', '5. volume'] 

class K:
    host = "https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={key}"

def download_json_from_API(symbol: str):
    try:
        url = K.host.format(symbol=symbol, key=access_token)
        req = requests.get(url)
        req.raise_for_status()
        data = req.json()
        full_filename = os.path.join(EXPORT_PATH, FILENAME.format(symbol=symbol))
        with open(full_filename, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Данные сохранены в {full_filename}")
    except requests.RequestException as e:
        print(f"ERROR with the request: {e}")

def connection_DB():
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"ERROR: {e}")
               
def parse_JSON(symbol: str):
    full_filename = os.path.join(EXPORT_PATH, FILENAME.format(symbol=symbol))
    with open(full_filename, "r") as json_file:
        json_data = json.load(json_file)
        json_data = json_data["Monthly Time Series"]
    return json_data
      
def create_table_for_ticker(symbol):
    con = connection_DB()
    cur = con.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS alphavantage;")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS alphavantage."{symbol}" (
            date_of_data date PRIMARY KEY,
            open_price numeric,
            high_price numeric,
            low_price numeric,
            close_price numeric,
            volume bigint
        );
    """)
    con.commit()
    cur.close()
    con.close()
    print(f'Таблица alphavantage."{symbol}" создана!')    
    
    
def load_to_DB(symbol):
    con = connection_DB()
    cur = con.cursor()
    
    rows_to_insert = []
    
    download_json_from_API(symbol)
    data = parse_JSON(symbol)    
    create_table_for_ticker(symbol)
    
    insert_query = f"""
        INSERT INTO alphavantage."{symbol}"
            (date_of_data, open_price, high_price, low_price, close_price, volume)
        VALUES ({', '.join(['%s'] * (len(columns)+1))})
        ON CONFLICT (date_of_data) DO NOTHING;
    """
    
    for date, values in data.items():
        # кортеж: (дата, open, high, low, close, volume)
        row = (date,) + tuple(values.get(col) for col in columns)
        rows_to_insert.append(row)
        
    cur.executemany(insert_query, rows_to_insert)
    con.commit() 
    print(f"Загружено {len(data)} записей в alphavantage.{symbol}")
    
if __name__ == "__main__":
    load_to_DB("AAPL")
    load_to_DB("IBM")
    load_to_DB("TSL")