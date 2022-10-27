'''
Libreria utilizada para la obtencion del dataset procedente de RAWG.io
'''

# %%
# Cargamos las librerias necesarias para realizar este proceso

import datetime as dt
import re
from difflib import SequenceMatcher
from dateutil.relativedelta import relativedelta
import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from fuzzywuzzy import process
import numpy as np
# %%
# Se definen una serie de funciones que se usaran durante todo el script


def not_null(original, new):
    '''
    Se actualiza el dataframe con los datos nuevos si existiesen
    '''
    if isinstance(new, list):
        return original
    if pd.isnull(new):
        return original
    return new


def column_merge(full_df, small_df, where='id'):
    '''
    Fusion de DataFrames en los que se actualiza informacion
    '''
    full_df = (
        full_df
        .merge(small_df, on=where, how='left', suffixes=('', '_'))
        )

    for col in [col[:-1] for col in full_df.columns if col[-1] == '_']:
        full_df[col] = (
            full_df
            .apply(
                lambda row: not_null(
                    row[col], row[f'{col}_']
                ),
                axis=1
            )
        )
        full_df.drop(f'{col}_', axis=1, inplace=True)

    return full_df


# %%
# Se define una sesion para realizar una serie de reintentos en caso de fallos
# a la hora de consultar las distintas urls utilizadas
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.2,
    status_forcelist=[500, 502, 503, 504]
)
session.mount('https://', HTTPAdapter(max_retries=retries))

# %%
# Se define una serie de parametros que nos resultaran de utilidad a lo largo
# de esta libreria
API_URL = 'https://api.rawg.io/api'
BASE_URL = 'https://rawg.io'

# %%
'''
Se definen las funciones que se usaran en el tratamiento de los datos de cada
uno de los campos a procesar
'''


def get_rawg_id(game_id, game, plat):
    '''
    Funcion utilizada para buscar el id dentro de RAWG del juego en cuestion
    '''
    game_info = [game_id]
    url = f'https://rawg.io/api/games?key={KEY_SEARCH}&search={game}'
    plat = PLAT_DICT.get(plat)
    if plat is not None:
        url += f'&platforms={plat}'
    try:
        result = session.get(url).json()['results'][0]

        rawg_name = result["name"]
        rawg_name_lower = rawg_name.lower()
        game_info.append(rawg_name)
        game_info.append(result["slug"])
        if 'Pokémon' in game:
            equal_name = True
            for word in re.sub(r"[&\:\-\!\?\,\.\#]", "", game).split():
                if word.lower() not in rawg_name_lower:
                    equal_name = False
                    break
        elif re.match(r'.*\(\d{4}\)$', rawg_name_lower):
            equal_name = SequenceMatcher(
                None, game.lower(), rawg_name_lower[:-6]
            ).ratio() >= 0.85
        else:
            equal_name = SequenceMatcher(
                None, game.lower(), rawg_name_lower
            ).ratio() >= 0.85
        game_info.append(equal_name)
        game_info.append(result["metacritic"])
        game_info.append(result["rating"])
        ratings = dict()
        for rat in result['ratings']:
            ratings[rat['title']] = rat['count']
        game_info.append(ratings)
        return game_info
    except:
        return [game_id, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN]


def update_info(game_slug):
    '''
    Funcion utilizada para actualizar la informacion de un juego
    '''
    game_info = [game_slug]
    url = f'{API_URL}/games/{game_slug}?key={KEY_UPDATE}'
    try:
        result = session.get(url).json()
        game_info.append(result['metacritic'])
        game_info.append(result['rating'])
        ratings = dict()
        for rat in result['ratings']:
            ratings[rat['title']] = rat['count']
        game_info.append(ratings)
        return game_info
    except:
        return [game_slug, np.NaN, np.NaN, np.NaN]


def obtain_devs(slug):
    '''
    Se obtiene la informacion de los devs de un juego
    '''
    try:
        url_devs = f'{API_URL}/games/{slug}/development-team?key={KEY_UPDATE}'
        devs = session.get(url_devs).json()['results']
        devs_names = []
        devs_advanced = []
        for dev in devs:
            devs_names.append(dev['name'])
            devs_advanced.append({
                'Name': dev['name'],
                'Slug': dev['name'],
                'Position': [pos['name'] for pos in dev['positions']]
            })
        return [slug, devs_names, devs_advanced]
    except:
        return [slug, [], []]


def update_process(rated_df):
    '''
    Se ejecuta el proceso de actualizacion de datos existentes
    '''
    up_df = (
        rated_df
        .loc[
            (
                rated_df['release_dates'] >=
                dt.datetime.today() - relativedelta(months=1)
                ) &
            (
                rated_df['release_dates'] <=
                dt.datetime.today()
                ) &
            (rated_df['RAWG_equal_name'] == 'True')
            ]
        .sort_values('release_dates')
        .drop_duplicates(subset='id')
        )

    updates_df = pd.DataFrame(
        up_df['RAWG_link'].map(update_info).tolist(),
        columns=['RAWG_link', 'MC_rating', 'RAWG_rating', 'RAWG_nreviews']
        )
    print('Info de RAWG actualizada')
    updates_df = (
        updates_df
        .merge(
            pd.DataFrame(
                up_df['RAWG_link'].map(obtain_devs).tolist(),
                columns=['RAWG_link', 'devs', 'advanced_devs']
                ),
            on='RAWG_link',
            how='left'
            )
        )
    print('Devs actualizados')
    rated_df = column_merge(rated_df, updates_df, 'RAWG_link')
    return rated_df


def obtain_new(rated_df):
    '''
    Se obtienen los datos de nuevos juegos
    '''
    rawg_df = (
        pd.DataFrame(
            rated_df
            .apply(
                lambda row: get_rawg_id(
                    row['id'], row['name'], row['platforms']
                    ),
                axis=1
            )
            .tolist(),
            columns=[
                'id', 'RAWG_name', 'RAWG_link', 'RAWG_equal_name',
                'MC_rating', 'RAWG_rating', 'RAWG_nreviews'
            ]
        )
    )
    print('Nueva informacion de RAWG adquirida')

    rawg_df = (
        rawg_df
        .merge(
            pd.DataFrame(
                rawg_df
                .loc[rawg_df['RAWG_equal_name'] == True]
                .drop_duplicates('RAWG_link')
                ['RAWG_link'].map(obtain_devs).tolist(),
                columns=['RAWG_link', 'devs', 'advanced_devs']
            ),
            on='RAWG_link',
            how='left'
        )
    )
    print('Nuevos devs adquiridos')

    return rawg_df

def get_rawg(rated_df, keys, update=True):
    '''
    Se define la funcion que obtendra datos desde RAWG
    '''
    global KEY_SEARCH
    KEY_SEARCH = keys[3]
    global KEY_UPDATE
    KEY_UPDATE = keys[2]

    if update:
        if len(rated_df.loc[rated_df['RAWG_equal_name'].isnull()]) == 0:
            return update_process(rated_df)

    platforms_df = pd.DataFrame(
        rated_df['platforms'].unique(),
        columns=['platforms']
    )

    plats = dict()
    for page in [1, 2]:
        for plat in session.get(
            f'{API_URL}/platforms?key={KEY_SEARCH}&page_size=40&page={page}'
        ).json()['results']:
            plats[plat['name']] = plat['id']

    choices = plats.keys()
    platforms_df['close_plat'] = (
        platforms_df['platforms']
        .apply(lambda x: process.extractOne(x, choices)[0])
    )

    platforms_df.loc[
        platforms_df['platforms']
        .isin([
            'Arcade', 'Legacy Mobile Device', 'Ouya', 'Windows Phone',
            'BlackBerry OS', 'N-Gage'
        ]),
        'close_plat'
    ] = ''
    platforms_df.loc[
        platforms_df['platforms']
        .isin([
            'MSX', 'FM-7', 'Sharp X1', 'ZX Spectrum', 'DOS', 'FM Towns',
            'Oculus Quest', 'Oculus Quest 2', 'Oculus Rift', 'SteamVR',
            'Windows Mixed Reality', 'Sharp X68000', 'TurboGrafx-16/PC Engine',
            'Google Stadia', 'Intellivision', 'ColecoVision',
            'BBC Microcomputer System'
        ]),
        'close_plat'
    ] = 'PC'
    platforms_df.loc[
        platforms_df['platforms']
        .isin([
            'Nintendo Entertainment System', 'Family Computer',
            'Family Computer Disk System'
        ]),
        'close_plat'
    ] = 'NES'

    platforms_df.loc[
        (platforms_df['platforms'] == 'PlayStation VR'), 'close_plat'
    ] = 'PlayStation 4'

    platforms_df.loc[
        platforms_df['platforms']
        .isin([
            'Super Nintendo Entertainment System', 'Super Famicom',
            'Satellaview'
        ]),
        'close_plat'
    ] = 'SNES'

    platforms_df.loc[
        (platforms_df['platforms'] == 'PlayStation Portable'), 'close_plat'
    ] = 'PSP'

    platforms_df.loc[
        (platforms_df['platforms'] == 'Sega Mega Drive/Genesis'), 'close_plat'
    ] = 'Genesis'

    rawg_plat = (
        pd.DataFrame.from_dict(plats, orient='index')
        .reset_index()
        .rename(columns={'index': 'close_plat', 0: 'plat_id'})
    )

    global PLAT_DICT
    PLAT_DICT = (
        platforms_df
        .merge(rawg_plat, on='close_plat')
        .drop('close_plat', axis=1)
        .set_index('platforms')
        .to_dict()['plat_id']
    )
    print('Relacion de plataformas con RAWG obtenida')

    if update:
        rated_df = update_process(rated_df)
        rawg_df = obtain_new(
            rated_df.loc[rated_df['RAWG_equal_name'].isnull()]
            )
        final_df = column_merge(rated_df, rawg_df, 'id')

    else:
        rawg_df = obtain_new(
            pd.DataFrame(
                rated_df
                .sort_values('release_dates')
                .drop_duplicates('id', keep='first')
                )
            )

        final_df = rated_df.merge(rawg_df, on='id', how='left')

    return final_df.drop_duplicates(['id', 'platforms'])
