import psycopg2
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

from dotenv import load_dotenv
import os

load_dotenv()
db_params = {
    "host": os.getenv('DB_HOST'),
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
}

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
        date = datetime.strptime(child.attrib['Date'], "%d.%m.%Y").date()
        item1 = int(child.find('Nominal').text)
        item2 = float(child.find('Value').text.replace(',', '.'))
        item3 = float(child.find('VunitRate').text.replace(',', '.'))
        parsed_rows.append((date, item1, item2, item3))
    return parsed_rows

def create_table_for_ticker(date_req1:str, date_req2:str, ticker:str):
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS foreign_currency_market_dynamic;")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS foreign_currency_market_dynamic."{ticker}_{date_req1}_{date_req2}" (
                    date_of_data date PRIMARY KEY,
                    nominal int,
                    value numeric,
                    vunitRate numeric
                );
            """)
    print(f'Таблица foreign_currency_market_dynamic."{ticker}_{date_req1}_{date_req2}" создана!')    

def load_to_db(date_req1:str, date_req2:str, ticker:str):
# def load_to_db(xml_data_parsed):
    schema_creation = "CREATE SCHEMA IF NOT EXISTS foreign_currency_market_dynamic;"
    
    create_table_querry = f"""
                CREATE TABLE IF NOT EXISTS foreign_currency_market_dynamic."{ticker}_{date_req1}_{date_req2}" (
                    date_of_data date PRIMARY KEY,
                    nominal int,
                    value numeric,
                    vunitRate numeric
                );"""
    
    insert_data_querry = f"""
        INSERT INTO foreign_currency_market_dynamic."{ticker}_{date_req1}_{date_req2}" 
            (date_of_data, nominal, value, vunitRate)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date_of_data) DO NOTHING
        """
    
    xml_data_parsed = parse_xml(get_data_xml(date_req1, date_req2, ticker))
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_creation)
            cur.execute(create_table_querry)
            print(f'Таблица foreign_currency_market_dynamic."{ticker}_{date_req1}_{date_req2}" создана!')    
            cur.executemany(insert_data_querry, xml_data_parsed)
            print(f"Inserted {cur.rowcount} records into the database.")
            

# create_table_for_ticker(date_req1="02.03.2001", date_req2="14.03.2001", ticker='R01235')
# load_to_db(date_req1="02.03.2001", date_req2="14.03.2001", ticker='R01235')
if __name__ == "__main__":
    load_to_db(date_req1="01.01.2001", date_req2="01.01.2025", ticker='R01235')
    
