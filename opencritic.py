'''
Libreria utilizada para la obtencion del dataset procedente de OpenCritic
'''

# %%
# Cargamos las librerias necesarias para realizar este proceso

import datetime as dt
from dateutil.relativedelta import relativedelta
import requests
from requests.adapters import HTTPAdapter, Retry
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from fuzzywuzzy import process
# %%
# Se definen una serie de funciones que se usaran durante todo el script


def number_or_not(string):
    '''
    Se comprueba si un string es un numero o no
    '''
    if string.isnumeric:
        return int(string)
    return np.NaN


def find_oc(game_id, name, plat):
    '''
    Encontramos el nombre de cada juego en Opencritic
    '''
    result = process.extractOne(name, games_plat[plat_dict.get(plat)])
    try:
        if result[1] >= 90:
            return [game_id, True, result[0]]
        return [game_id, False, result[0]]
    except:
        return [game_id, np.NaN, np.NaN]


def not_null(original, new):
    '''
    Nos quedamos con los datos nuevos si existiesen
    '''
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
# Definimos una sesion para realizar una serie de reintentos en caso de fallos
# a la hora de consultar las distintas urls utilizadas
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.2,
    status_forcelist=[500, 502, 503, 504]
)
session.mount('https://', HTTPAdapter(max_retries=retries))

# %%
# Definimos una serie de parametros que nos resultaran de utilidad a lo largo
# de esta libreria
plat_dict = {
    'PC (Microsoft Windows)': 'PC',
    'Xbox One': 'XB1',
    'Mac': 'PC',
    'Linux': 'PC',
    'Nintendo Switch': 'Switch',
    'PlayStation 4': 'PS4',
    'PlayStation Vita': 'Vita',
    'Nintendo 3DS': '3DS',
    'Wii U': 'Wii-U',
    'Web browser': 'PC',
    'Xbox Series X|S': 'XBXS',
    'New Nintendo 3DS': '3DS',
    'Oculus Quest': 'Oculus',
    'Oculus Quest 2': 'Oculus',
    'Oculus Rift': 'Oculus',
    'PlayStation VR': 'PSVR',
    'SteamVR': 'PC',
    'Windows Mixed Reality': 'PC',
    'PlayStation 5': 'PS5',
    'Oculus VR': 'Oculus',
    'Google Stadia': 'Stadia'
}
GAME_URL = 'https://opencritic.com/game/'

# %%
'''
Definimos las funciones que se usaran en el tratamiento de los datos de cada
uno de los campos a procesar
'''


def get_oc_links(games_df):
    '''
    Obtenemos los links disponibles en OpenCritic
    '''
    url_base = 'https://opencritic.com/browse/all/all-time/date?page='
    titles = []
    links = []
    plats = []
    dates = []
    limit_date = games_df['first_release_date'].min()
    page = 1
    date = dt.datetime.today()
    while date > limit_date:
        soup = BeautifulSoup(session.get(f'{url_base}{page}').text)
        for game in soup.find_all(
            attrs={'class': 'row no-gutters py-2 game-row align-items-center'}
        ):
            game_title = game.find('div', attrs={'class': 'game-name col'})
            titles.append(game_title.text)
            links.append(game_title.find('a', href=True)['href'][6:])
            plats.append(
                game
                .find('div', attrs={'class': 'platforms col-auto'})
                .text.strip().split(', ')
            )
            date = pd.to_datetime(
                game.find(
                    'div',
                    attrs={'class': 'first-release-date col-auto show-year'}
                )
                .text
            )
            dates.append(date)
        if max([
            int(a['href'].split('/browse/all/all-time/date?page=')[1])
            for a in soup.select('a[href^="/browse/all/all-time/date?page="]')
        ]) < page+1:
            break
        page += 1

    oc_df = pd.DataFrame(
        np.array([titles, links, plats, dates], dtype=object).T.tolist(),
        columns=['OC_name', 'OC_link', 'OC_plat', 'OC_date']
    ).explode('OC_plat')

    global GAMES_PLAT
    GAMES_PLAT = dict()
    for plat in oc_df['OC_plat'].unique():
        GAMES_PLAT[plat] = oc_df.loc[
            oc_df['OC_plat'] == plat, 'OC_name'
            ].tolist()

    return oc_df


def get_rating(game_id, oc_id):
    '''
    Conocido el id de un juego, se obtiene su valoracion
    '''
    result = [game_id]
    try:
        soup = BeautifulSoup(session.get(f'{GAME_URL}{oc_id}').text)
    except:
        return [game_id, 0, 0]
    try:
        result.append(int(soup.find('div', attrs={'class': 'inner-orb'}).text))
    except:
        result.append(0)
    try:
        result.append(
            int(soup.select(f'a[href^="/game/{oc_id}"]')[0].text.split()[2])
        )
    except:
        result.append(0)
    return result


def get_oc(hltb_df, update=True):
    '''
    Definimos la funcion que obtendra datos desde OpenCritic
    '''
    if update:
        update_df = (
            hltb_df.loc[
                (hltb_df['release_dates'] >=
                 dt.datetime.today() - relativedelta(months=3)
                 ) &
                (hltb_df['release_dates'] <= dt.datetime.today()) &
                (hltb_df['platforms'].isin(plat_dict.keys())) &
                (hltb_df['OC_equal_name'] == 'True')
            ]
            .sort_values('release_dates')
            .drop_duplicates('id')
        )

        torate_df = (
            hltb_df.loc[
                (hltb_df['first_release_date'] >=
                 dt.datetime(year=2013, month=11, day=1)
                 ) &
                (hltb_df['platforms'].isin(plat_dict.keys())) &
                (hltb_df['OC_equal_name'].isnull())
            ]
            .sort_values('release_dates')
            .drop_duplicates('id')
            )

    else:
        torate_df = (
            hltb_df.loc[
                (hltb_df['first_release_date'] >=
                 dt.datetime(year=2013, month=11, day=1)
                 ) &
                (hltb_df['platforms'].isin(plat_dict.keys()))
            ]
            .sort_values('release_dates')
            .drop_duplicates('id')
            )
    print('Juegos a obtener en OpenCritic definidos')

    oc_df = get_oc_links(torate_df)
    print('Nuevos links de OpenCritic obtenidos')

    if len(torate_df) > 0:
        reviews_df = (
            pd.DataFrame(
                torate_df
                .apply(lambda row: find_oc(
                    row['id'], row['name'], row['platforms']
                ), axis=1)
                .tolist(),
                columns=['id', 'OC_equal_name', 'OC_name']
            )
            .merge(
                oc_df[['OC_name', 'OC_link']].drop_duplicates(),
                on='OC_name'
            )
        )
    print('Links de OpenCritic obtenidos')

    if update:
        if len(torate_df) > 0:
            reviews_df = pd.concat([
                reviews_df,
                update_df[['id', 'OC_equal_name', 'OC_name', 'OC_link']]
                ])
        else:
            reviews_df = update_df[
                ['id', 'OC_equal_name', 'OC_name', 'OC_link']
                ]

    reviews_df = (
        reviews_df
        .merge(
            pd.DataFrame(
                reviews_df.apply(
                    lambda row: get_rating(row['id'], row['OC_link']), axis=1
                )
                .tolist(),
                columns=['id', 'OC_rating', 'OC_nreviews']
            ),
            on='id',
            how='left'
        )
    )

    if update:
        rated_df = column_merge(hltb_df, reviews_df)

    else:
        rated_df = hltb_df.merge(reviews_df, on='id', how='left')

    print('Valoraciones obtenidas')
    return rated_df
