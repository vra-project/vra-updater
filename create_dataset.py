'''
Programa utilizado para realizar la ETL
- Extracción de Datos de las 4 bases de datos
- Transformación y limpieza para su posterior uso con ML
- Carga en bucket S3
'''
# %%
# Cargamos las librerías necesarias para realizar este proceso

from configparser import ConfigParser
import warnings
import datetime as dt
import pandas as pd
from botocore.exceptions import ClientError
from igdb import get_igdb
from hltb import get_hltb
from opencritic import get_oc
from rawg import get_rawg
# %%
# Cargamos las claves necesarias para utilizar a lo largo del proceso
# Se usarán keys para acceder a las APIs de IGDB y RAWG
# También necesitamos claves de acceso a nuestro servidor de AWS

config = ConfigParser()
config.read('secrets.toml', encoding='utf-8')

AWS_ACCESS_KEY_ID = config['AWS']['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['AWS']['aws_secret_access_key']
AWS_SESSION_TOKEN = config['AWS']['aws_session_token']

CLIENT_ID = config['IGDB']['client_id']
CLIENT_SECRET = config['IGDB']['client_secret']

RAWG_KEYS = [
    config['RAWG']['primary_key'],
    config['RAWG']['secondary_key'],
    config['RAWG']['third_key'],
    config['RAWG']['fourth_key']
]

BUCKET_S3 = config['AWS']['bucket_s3']
FILE_NAME = 'games'
NEW_FILE_NAME = FILE_NAME

warnings.filterwarnings('ignore')

# %%
# En primer lugar, obtendremos los datos de IGDB
igdb_df = get_igdb(CLIENT_ID, CLIENT_SECRET)
# Después, conectamos con HLTB
hltb_df = get_hltb(igdb_df, False)
# Realizamos la conexion con Opencritic
rated_df = get_oc(hltb_df, False)
# Realizamos la conexion con RAWG
final_df = get_rawg(rated_df, RAWG_KEYS, False)

# %%
# Creamos un fichero con el DataFrame resultante
try:
    final_df.reset_index(drop=True).astype(str).to_feather(
        f'{BUCKET_S3}/{NEW_FILE_NAME}.feather',
        compression='lz4',
        storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret": AWS_SECRET_ACCESS_KEY,
            "token": AWS_SESSION_TOKEN
        }
    )
    print('Dataset creado en S3')
    legacy_df.reset_index(drop=True).astype(str).to_feather(
        (
            f'{BUCKET_S3}/{NEW_FILE_NAME}_'
            f'{dt.datetime.today().strftime("%y-%m-%d-%H")}.feather'
            ),
        compression='lz4'
    )
except OSError or ClientError:
    print('No se ha podido guardar el dataset')
