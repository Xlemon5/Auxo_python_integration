import paramiko
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import json

load_dotenv()
db_params = {
    "host": os.getenv('DB_HOST'),
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
}

sftp_params = {
    "host": os.getenv('SFTP_HOST'),
    "user": os.getenv('SFTP_USERNAME'),
    "path_to_pkey": os.getenv('SFTP_PATH_TO_PKEY'),
    "pass_for_pkey": os.getenv('SFTP_PASSWORD_FOR_PKEY')
}

file_path = "/Users/ilya/Desktop/AUXO_python_integration/data/"
file_name = "{symbol}.csv"

def get_from_db(table:str, symbol:str):
    querry = f"""
        SELECT * FROM {table}."{symbol}";
    """
    
    with psycopg2.connect(**db_params) as conn:
        dataframe = pd.read_sql_query(querry, conn)
    
    if dataframe is not None:
        dataframe.to_csv(os.path.join(file_path, file_name.format(symbol=symbol)), sep=',', index=False)
        print(f"Экспортировано: {file_path}{file_name.format(symbol=symbol)}")
    else:
        print(f"Нет данных для {symbol}, CSV не создан.")

def create_table_for_ticker(symbol):
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
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
    print(f'Таблица alphavantage."{symbol}" создана!')    

columns = ['1. open', '2. high', '3. low', '4. close', '5. volume'] 
def put_to_DB(symbol:str):
    create_table_for_ticker(symbol)
    df = pd.read_csv(os.path.join(file_path, file_name.format(symbol=symbol)))

    insert_querry = f"""
        INSERT INTO alphavantage."{symbol}"
            (date_of_data, open_price, high_price, low_price, close_price, volume)
        VALUES ({', '.join(['%s'] * (len(columns)+1))})
        ON CONFLICT (date_of_data) DO NOTHING;
    """
    
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for row in df.itertuples(index=False, name=None):
                cur.execute(
                    insert_querry, 
                    row
                )
    print(f"Загружено {len(df)} записей в alphavantage.{symbol}")
    
def put_file_to_SFTP(table:str, symbol:str):
    transport = paramiko.Transport((os.getenv('SFTP_HOST'), 22))
    transport.connect(
        username=os.getenv('SFTP_USERNAME'),
        password=os.getenv('SFTP_PASS_CON')
    )
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        sftp.stat(table)  # информация о папке
    except FileNotFoundError:
        sftp.mkdir(table)  # Если папки нет, то, создаем
        
    local_path = os.path.join(file_path, file_name.format(symbol=symbol))
    remote_path = f'/{table}/{symbol}.csv'
    
    sftp.put(local_path, remote_path)
    
    print(f"Файл {local_path} загружен в {remote_path}") 
    
    sftp.close()
    transport.close()

def get_file_from_SFTP(table:str, symbol:str):
    transport = paramiko.Transport((os.getenv('SFTP_HOST'), 22))
    transport.connect(
        username=os.getenv('SFTP_USERNAME'),
        password=os.getenv('SFTP_PASS_CON')
    )
    sftp = paramiko.SFTPClient.from_transport(transport)
          
    local_path = os.path.join(file_path, file_name.format(symbol=symbol))
    remote_path = f'/{table}/{symbol}.csv'        

    sftp.get(remote_path, local_path) 
    print(f"Файл {remote_path} скачан в {local_path}")
    
    sftp.close()
    transport.close()
    