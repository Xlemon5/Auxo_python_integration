import requests
import json
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError

from dotenv import load_dotenv
import os

# mode = ["capsules", "company", "cores", "crew", "dragons", "landpads", "launches", "launchpads", "payloads", "roadster", "rockets", "ships", "starlink"]
# URL = f"https://api.spacexdata.com/v4/{mode[1]}"

load_dotenv()
db_params = {
    "host": os.getenv('DB_HOST'),
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
}

class K:
    host = "https://api.spacexdata.com/v4"

def get_data_from_url(url: str):
    """Функция загрузки данных из API"""
    request = requests.get(url, timeout=30)
    if request.status_code == 404:
        raise AttributeError("Неверное значение URL-адреса")
    return request.json()

def check_DB_connection():
    """Проверка соединения с БД"""
    try:
        connection = psycopg2.connect(**db_params)
        connection.close()
        print("Соединение с бд успешно установлено!")
        return True
    except OperationalError as e:
        print(f"Ошибка соединения с базой данных: {e}")

# check_DB_connection()

def load_data_to_DB(endpoint, table, columns):
    """
        Загрузка данных в бд
        endpoint: str, for example - crew
        table: str, for example - spaceX.crew
        columns: tuple/list, for example: (name, agency, etc.)
    
    """
    json_data = get_data_from_url(f"{K.host}/{endpoint}") 
    
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    insert_query = f"""
        INSERT INTO {table}
            ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        ON CONFLICT (id) DO NOTHING;
    """

    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for row in json_data:
                data_to_insert = tuple(row.get(col) for col in columns)
                cur.execute(insert_query, data_to_insert)        
    print(f"Загружено {len(json_data)} записей в {table}")

check_DB_connection()
load_data_to_DB(
    endpoint="crew",
    table="spaceX.crew",
    columns=("name", 
             "agency", 
             "image", 
             "wikipedia", 
             "launches", 
             "status", 
             "id")
)

load_data_to_DB(
    endpoint="cores",
    table="spaceX.cores",
    columns=(
        "block", 
        "reuse_count", 
        "rtls_attempts", 
        "rtls_landings", 
        "asds_attempts", 
        "asds_landings", 
        "last_update", 
        "launches", 
        "serial",
        "status",
        "id")
)
