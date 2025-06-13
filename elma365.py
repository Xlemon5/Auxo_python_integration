import requests
import psycopg2
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

from dotenv import load_dotenv
import os

url = 'https://r5jfdcan5aqau.elma365.ru/pub/v1/app/citaty/counterparty_info/list'

load_dotenv()
headers = {
    'Authorization': os.getenv('ELMA_Authorization')
}

db_params = {
    "host": os.getenv('DB_HOST'),
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
}

body = {
    "active": True
}

def get_list_of_elements():
    req = requests.post(url=url, headers=headers, json=body)
    if req.status_code == 404:
        raise AttributeError("Неверное значение URL-адреса")
    return req.json()       #returns dict

def parse_json(json_data):
    json_data = json_data['result']['result']   
    parse_data = []
    for item in json_data:
        parse_data.append((item['inn'], item['phone_number'][0]['tel'], item['fio_head']))
    return parse_data

def load_to_db():
    data_to_insert = parse_json(get_list_of_elements())
    
    schema_creation = """
        CREATE SCHEMA IF NOT EXISTS elma;
    """
    
    table_creation = """
        CREATE TABLE IF NOT EXISTS elma.list_of_applications_elements(
            id serial PRIMARY KEY,
            inn text,
            telephone text,
            fio text
        )
    """
    
    insert_data_querry = """
        INSERT INTO elma.list_of_applications_elements (inn, telephone, fio)
            VALUES (%s, %s, %s);
    """
    
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_creation)
            cur.execute(table_creation)
            cur.executemany(insert_data_querry, data_to_insert)
    print(f"Загружено строк: {len(data_to_insert)}")
    

# load_to_db()

url_create_applicataion = 'https://r5jfdcan5aqau.elma365.ru/pub/v1/app/citaty/counterparty_info/create'

body_to_create_application = {
  "context": {
    "inn": "5566778899",
    "fio_head": "Петров Петр Петрович",
    "__name": "ЗерноТрейд",
    "e_mail": [
      {
        "type": "main",
        "email": "petrov@zernotrade.ru",
        "isValid": True
      }
    ],
    "head_company": False,
    "phone_number": [
      {
        "ext": "123",
        "tel": "+79990001122",
        "type": "work",
        "isValid": True
      }
    ]
  }
}


def create_application():
    resp = requests.post(url=url_create_applicataion, headers=headers, json=body_to_create_application)
    print(resp.status_code)
    print(resp.json())


# create_application()

print(get_list_of_elements())









