import requests
import json
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
db_params = {
    "host": os.getenv('DB_HOST'),
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
}

def connection_DB():
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"Error {e}")
        return None

def get_data_DB(symbol: str):
    conn = connection_DB()
    if conn is None:
        print("Не удалось подключиться к БД.")
        return None

    command_toDB = f"""
        SELECT * FROM alphavantage."{symbol}";
    """
    try:
        data_frame = pd.read_sql_query(command_toDB, conn)
        return data_frame
    except Exception as e:
        print(f"Ошибка чтения из БД: {e}")
        return None
    finally:
        conn.close()  

file_path = "/Users/ilya/Desktop/AUXO_python_integration/data/"
file_name = "{symbol}.csv"

def dataFrame_to_csv(symbol: str):
    data = get_data_DB(symbol)
    if data is not None:
        data.to_csv(os.path.join(file_path, file_name.format(symbol=symbol)), sep=',', index=False)
        print(f"Экспортировано: {file_path}{file_name.format(symbol=symbol)}")
    else:
        print(f"Нет данных для {symbol}, CSV не создан.")

if __name__ == "__main__":
    dataFrame_to_csv("AAPL")
    dataFrame_to_csv("IBM")
    dataFrame_to_csv("TSLA")
