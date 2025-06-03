import logging
import requests
import json
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError

from dotenv import load_dotenv
import os

logging.basicConfig(
    filename='alphavantage_loader.log',
    level=logging.INFO,  # INFO и выше попадёт в лог
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


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
        logger.info(f"[{symbol}] Данные сохранены в {full_filename}")
        return True
    except requests.RequestException as e:
        logger.error(f"[{symbol}] ERROR with the request: {e}")
        return False
    except Exception as e:
        logger.error(f"[{symbol}] General error during download: {e}")
        return False

def connection_DB():
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        return None

def parse_JSON(symbol: str):
    try:
        full_filename = os.path.join(EXPORT_PATH, FILENAME.format(symbol=symbol))
        with open(full_filename, "r") as json_file:
            json_data = json.load(json_file)
            json_data = json_data["Monthly Time Series"]
        logger.info(f"[{symbol}] JSON успешно распарсен")
        return json_data
    except Exception as e:
        logger.error(f"[{symbol}] Ошибка при парсинге JSON: {e}")
        return None

def create_table_for_ticker(symbol):
    con = connection_DB()
    if not con:
        logger.error(f"[{symbol}] Не удалось создать подключение к БД")
        return False
    try:
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
        logger.info(f'[{symbol}] Таблица alphavantage."{symbol}" создана!')
        return True
    except Exception as e:
        logger.error(f'[{symbol}] Ошибка при создании таблицы: {e}')
        return False
    finally:
        con.close()    

def load_to_DB(symbol):
    try:
        if not download_json_from_API(symbol):
            logger.error(f"[{symbol}] Пропускаю загрузку в БД из-за ошибки при скачивании.")
            return
        data = parse_JSON(symbol)
        if data is None:
            logger.error(f"[{symbol}] Пропускаю загрузку в БД из-за ошибки при парсинге.")
            return
        if not create_table_for_ticker(symbol):
            logger.error(f"[{symbol}] Пропускаю загрузку в БД из-за ошибки при создании таблицы.")
            return

        con = connection_DB()
        if not con:
            logger.error(f"[{symbol}] Пропускаю загрузку в БД из-за ошибки при подключении к БД.")
            return
        cur = con.cursor()
        
        insert_query = f"""
            INSERT INTO alphavantage."{symbol}"
                (date_of_data, open_price, high_price, low_price, close_price, volume)
            VALUES ({', '.join(['%s'] * (len(columns)+1))})
            ON CONFLICT (date_of_data) DO NOTHING;
        """

        rows_to_insert = []
        for date, values in data.items():
            row = (date,) + tuple(values.get(col) for col in columns)
            rows_to_insert.append(row)
        
        cur.executemany(insert_query, rows_to_insert)
        con.commit()
        logger.info(f"[{symbol}] Загружено {len(rows_to_insert)} записей в alphavantage.{symbol}")
        cur.close()
        con.close()
    except Exception as e:
        logger.error(f"[{symbol}] Ошибка при загрузке данных в БД: {e}")

# Используй:
if __name__ == "__main__":
    load_to_DB("AAPL")
    load_to_DB("IBM")
    load_to_DB("TSL")
    load_to_DB("BM")
