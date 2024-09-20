import os
import sys
from sqlalchemy import create_engine, Integer, Float, String, TIMESTAMP, BOOLEAN
import pandas as pd
from dotenv import load_dotenv

def load_environment_variables():
    """Cargar las variables de entorno desde el archivo .env."""
    print("va a buscar los parametros")
    load_dotenv()
    return {
        'host': os.getenv("REDSHIFT_HOST"),
        'port': os.getenv("REDSHIFT_PORT"),
        'dbname': os.getenv("REDSHIFT_DB"),
        'user': os.getenv("REDSHIFT_USER"),
        'password': os.getenv("REDSHIFT_PASSWORD")
    }

def create_db_engine(env_vars):
    """Crear un motor de base de datos usando las variables de entorno."""
    conn_str = (
        f"redshift+redshift_connector://{env_vars['user']}:{env_vars['password']}@{env_vars['host']}:{env_vars['port']}/{env_vars['dbname']}"
    )
    return create_engine(conn_str)

def load_data(file_path):
    """Cargar datos desde un archivo parquet."""
    return pd.read_parquet(file_path)

def preprocess_data(data):
    """Preprocesar los datos según las necesidades."""
    data['segments_sector_1'] = data['segments_sector_1'].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
    data['segments_sector_2'] = data['segments_sector_2'].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
    data['segments_sector_3'] = data['segments_sector_3'].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
    return data

def define_dtype():
    """Definir los tipos de datos para la base de datos."""
    return {
        'meeting_key': Integer,
        'session_key': Integer,
        'driver_number': Integer,
        'i1_speed': Float,
        'i2_speed': Float,
        'st_speed': Float,
        'date_start': TIMESTAMP,
        'lap_duration': Float,
        'is_pit_out_lap': BOOLEAN,
        'duration_sector_1': Float,
        'duration_sector_2': Float,
        'duration_sector_3': Float,
        'segments_sector_1': String(200),
        'segments_sector_2': String(200),
        'segments_sector_3': String(200),
        'lap_numbe': Integer
    }

def save_to_database(data, engine, table_name, dtype):
    """Guardar los datos en la base de datos."""
    data.to_sql(table_name, engine, index=False, if_exists='replace', dtype=dtype)

def cargar_data():
    path=os.getcwd()
    """Función principal para ejecutar el flujo de trabajo."""
    file_path = (f"{path}/raw_data/data_9598.json")
    output_file_path = (f"{path}/raw_data/salida.txt")
    with open(output_file_path, 'w') as f:
        sys.stdout = f
    print("inicio del proceso")
    table_name = 'laps'
    env_vars = load_environment_variables()
    print("cargo las variables")
    engine = create_db_engine(env_vars)
    print("creo el motor de base")
    data = load_data(file_path)
    data = preprocess_data(data)
    dtype = define_dtype()
    save_to_database(data, engine, table_name, dtype)
    print( print("Grabo los valores"))
    sys.stdout = sys.__stdout__
