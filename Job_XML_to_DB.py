import psycopg2
import pandas as pd
import requests
import xml.etree.ElementTree as ET

localpath = 'C:\\Users\\a550940\\YandexDisk\\_Atos\\trainings\\_my_trainings\\Python_Loginom\\L_5_XML_DB'

def get_connect():
	db_params = {
		"host": "localhost",        # Database host
		"database": "postgres",   # Replace with your database name
		"user": "postgres",    # Replace with your database username
		"password": "5",  # Replace with your database password
        'port': '5432'
	}
	try:
		# Establish the connection
		conn = psycopg2.connect(**db_params)
		return conn
	except Exception as e:
		print(f"An error occurred: {e}")
 
def fetch_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Data received. Status code: {response.status_code}")
        return response.content  # Return the XML content
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return None
 
def parse_xml_data(xml_data):
    root = ET.fromstring(xml_data)
    parsed_data = []
    for item in root.findall('Valute'):  
        field1 = item.find('CharCode').text
        field2 = item.find('Name').text
        field3 = item.find('Value').text
        parsed_data.append((field1, field2, field3))
    return parsed_data
 
def insert_data_to_db(data):
    try:
        conn = get_connect()
        cursor = conn.cursor()
        print(f"Data for insert: {data}")
        insert_query = "INSERT INTO cbr_currency_rates (code, curr_name, rate) VALUES (%s, %s, %s)"  # Adjust per your table structure
        cursor.executemany(insert_query, data)
        conn.commit()
        print(f"Inserted {cursor.rowcount} records into the database.")
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()
        conn.close()
 
if __name__ == "__main__":
	# API URL
	api_url = "http://www.cbr.ru/scripts/XML_daily.asp?date_req=02/03/2002"  # Replace with your API URL
	xml_data = fetch_data_from_api(api_url)
	if xml_data:
		parsed_data = parse_xml_data(xml_data)
		insert_data_to_db(parsed_data)
	