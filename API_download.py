import requests
import json
import sys
import datetime
from sys import getdefaultencoding

print(datetime.datetime.now(), " ===== Start of ", sys.argv[0], " script.")


access_token = "d8c8ffd64019538a34c601513e921203"

export_path = "/Users/ilya/Desktop/AUXO_python_integration/"
filename = 'weather_sample'

lat = '33.44'
lon = '-94.04'

mainUrl = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={access_token}"

print(" ===== URL is - ", mainUrl)

req = requests.get(mainUrl, stream=True)

print(" ===== Status is - ", req.status_code)

if req.status_code == 200:
	
	json_data = json.loads(req.text)

	print(" ===== Write data into - ", export_path + filename + ".json")
	
	# Write the JSON data to a file14
	with open(export_path + filename, 'w') as json_file:
		json.dump(json_data, json_file, indent=4)
	
	print(f"Data saved to {filename}")

if req.status_code == 404:
	print(datetime.datetime.now()," ===== Please check the URL")		


print(datetime.datetime.now()," ===== End downloading")			
		