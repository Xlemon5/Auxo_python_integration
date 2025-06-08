import paramiko
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

sftp_params = {
    "host": os.getenv('SFTP_HOST'),
    "user": os.getenv('SFTP_USERNAME'),
    "path_to_pkey": os.getenv('SFTP_PATH_TO_PKEY'),
    "pass_for_pkey": os.getenv('SFTP_PASSWORD_FOR_PKEY')
}

file_path = "/Users/ilya/Desktop/AUXO_python_integration/data/"
file_name = "{symbol}.csv"

def get_data_from_db(table:str, symbol:str):
    querry = f"""
        SELECT * FROM {table}."{symbol}";
    """

    with psycopg2.connect(**db_params) as conn:
        dataframe = pd.read_sql_query(querry,conn)
    
    if dataframe is not None:
        dataframe.to_csv(os.path.join(file_path, file_name.format(symbol=symbol)), sep=',', index=False)
        print(f"Экспортировано: {file_path}{file_name.format(symbol=symbol)}")
    else:
        print(f"Нет данных для {symbol}, CSV не создан.")


def load_file_to_SFTP():
    transport = paramiko.Transport(('192.168.0.216', 22))
    transport.connect(
        username='XLMN',
        password='xlmn221847'
    )
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put('/Users/ilya/Desktop/AUXO_python_integration/data/cores.csv', '/spacex/cores.csv') 
    sftp.close()
    transport.close()


    
    