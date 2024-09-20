import json
import requests
from datetime import datetime


def extraer_data(exec_date, path):
    date = datetime.strptime(exec_date, "%Y-%m-%d %H")
    json_path = (
        f"{path}/raw_data/data_9598.json"
    )
    url = "https://api.openf1.org/v1/laps"
    session = 9598
    try:
        #response = requests.get(url, headers=headers)
        params = {
            "session_key": session
        }
        response = requests.get(url, params=params)
        if response:
            data = response.json()
            with open(
                json_path,
                "w",
            ) as json_file:
                json.dump(data, json_file)
    except ValueError as e:
        raise e
