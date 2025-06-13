import psycopg2
import pandas as pd
import requests
import xml.etree.ElementTree as ET

from dotenv import load_dotenv
import os

load_dotenv()
db_params = {
    "host": os.getenv('DB_HOST'),
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
}

EXPORT_PATH = "/Users/ilya/Desktop/AUXO_python_integration/data"

url = 'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={date_req1}&date_req2={date_req2}&VAL_NM_RQ={ticker}'

def get_data_xml(date_req1:str, date_req2:str, ticker:str):
    req = requests.get(url.format(date_req1 = date_req1, date_req2 = date_req2, ticker = ticker))
    if req.status_code == 200:
        print(f"Data recived successfully. Code {req.status_code}")
        return req.content
    else:
        print(f"Failed to retrieve data. Code {req.status_code}")

def parse_xml(xml_data):
    parsed_rows = []
    root = ET.fromstring(xml_data)
    for child in root.findall('Record'):
        date = child.attrib['Date']
        item1 = child.find('Nominal').text
        item2 = child.find('Value').text
        item3 = child.find('VunitRate').text
        parsed_rows.append((date, item1, item2, item3))
    return parsed_rows

def create_table_for_ticker(date_req1:str, date_req2:str, ticker:str):
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS Foreign_Currency_Market_Dynamic;")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS Foreign_Currency_Market_Dynamic."{ticker}_{date_req1}_{date_req2}" (
                    date_of_data date PRIMARY KEY,
                    nominal int,
                    value numeric,
                    vunitRate numeric
                );
            """)
    print(f'Таблица Foreign_Currency_Market_Dynamic."{ticker}_{date_req1}_{date_req2}" создана!')    

def load_to_db(xml_data):
    
    insert_querry = """
        INSERT INTO 
    """
    
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            cur.executemany()
    

create_table_for_ticker(date_req1="01.01.2001", date_req2="05.01.2001", ticker='R01235')
create_table_for_ticker(date_req1="02.03.2001", date_req2="14.03.2001", ticker='R01235')
# load_to_db()
