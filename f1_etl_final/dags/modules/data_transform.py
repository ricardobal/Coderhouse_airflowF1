import json
import pandas as pd
from datetime import datetime


def transformar_data(exec_date, path):
    print(f"Transformando la data para la fecha: {exec_date}")
""" 
    date = datetime.strptime(exec_date, "%Y-%m-%d %H")
    json_path = (
        f"{path}/raw_data/data__9598.json"
    )
    csv_path = (
        f"{path}/processed_data/data__9598.csv"
    )

    with open(json_path, "r") as json_file:
        loaded_data = json.load(json_file)
    
# def preprocess_data(data):
#     "Preprocesar los datos según las necesidades."
#     data['segments_sector_1'] = data['segments_sector_1'].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
#     data['segments_sector_2'] = data['segments_sector_2'].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
#     data['segments_sector_3'] = data['segments_sector_3'].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
#     return data



 """