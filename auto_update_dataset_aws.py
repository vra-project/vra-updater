'''
Programa utilizado para realizar la ETL
- Extracción de Datos de las 4 bases de datos
- Transformación y limpieza para su posterior uso con ML
- Carga en bucket S3
'''
# %%
# Se definen las librerías necesarias para realizar este proceso

from configparser import ConfigParser
import warnings
import datetime as dt
import pandas as pd
from botocore.exceptions import ClientError
from igdb import update_igdb
from hltb import get_hltb
from opencritic import get_oc
from rawg import get_rawg
# %%
# Se cargan las claves necesarias para utilizar a lo largo del proceso
# Se usaran keys para acceder a las APIs de IGDB y RAWG
# Tambien se necesitan claves de acceso al S3 de AWS

config = ConfigParser()
config.read('secrets.toml', encoding='utf-8')

CLIENT_ID = config['IGDB']['client_id']
CLIENT_SECRET = config['IGDB']['client_secret']

RAWG_KEYS = [
    config['RAWG']['primary_key'],
    config['RAWG']['secondary_key'],
    config['RAWG']['third_key'],
    config['RAWG']['fourth_key']
]

BUCKET_S3 = config['AWS']['bucket_s3']
FOLDER = 'dataset'
FILE_NAME = 'games'
NEW_FILE_NAME = FILE_NAME

warnings.filterwarnings('ignore')
# %%
# Se carga el dataset existente
# En este paso también se transformaran las fechas y otra serie de datos que
# se requeriran más adelante


try:
    legacy_df = (
        pd.read_feather(
            f'{BUCKET_S3}/{FOLDER}/{FILE_NAME}.feather',
        )
    )
    print('Dataset cargado correctamente desde S3')
except OSError or ClientError:
    print('No se ha podido cargar el dataset')

legacy_df = (
    legacy_df
    .assign(
        first_release_date=lambda df: pd.to_datetime(df['first_release_date']),
        release_dates=lambda df: pd.to_datetime(df['release_dates']),
        updated_at=lambda df: pd.to_datetime(df['updated_at'])
        )
    )
legacy_df['id'] = legacy_df['id'].astype(int)

# %%
# En primer lugar, se obtienen las novedades en IGDB
igdb_df = update_igdb(legacy_df, CLIENT_ID, CLIENT_SECRET)
# Después, se conecta con HLTB
hltb_df = get_hltb(igdb_df, True)
# Se realiza la conexion con Opencritic
rated_df = get_oc(hltb_df, True)
# Se realiza la conexion con RAWG
final_df = get_rawg(rated_df, RAWG_KEYS, True)

# %%
# Se crea un fichero con el DataFrame resultante
try:
    final_df.reset_index(drop=True).astype(str).to_feather(
        f'{BUCKET_S3}/{FOLDER}/{NEW_FILE_NAME}.feather',
        compression='lz4'
    )
    print('Dataset creado en S3')
    legacy_df.reset_index(drop=True).astype(str).to_feather(
        (
            f'{BUCKET_S3}/{FOLDER}/{NEW_FILE_NAME}_'
            f'{dt.datetime.today().strftime("%y-%m-%d-%H")}.feather'
            ),
        compression='lz4'
    )
except OSError or ClientError:
    print('No se ha podido guardar el dataset')
