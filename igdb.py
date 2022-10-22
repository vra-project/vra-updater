'''
Libreria utilizada para la obtencion del dataset procedente de IGDB
'''

# %%

# Cargamos las librerias necesarias para realizar este proceso
import datetime as dt
from time import sleep
import requests
from requests.adapters import HTTPAdapter, Retry
import numpy as np
import pandas as pd
from iso3166 import countries

# %%
# Se definen una serie de funciones que se usaran durante todo el script


def col_of_list(
    d_f, col_name, request_name, fields, explode=True, give_small=False
):
    '''
    Sirve para hacer una consulta a la API de todos los valores en la lista
    comprendida en cada uno de los valores de la columna solicitada
    '''
    if explode:
        small_df = d_f[['id', col_name]].explode(col_name)
    else:
        small_df = d_f[['id', col_name]]
    col_list = []
    col_array = tuple(small_df[col_name].dropna().unique().astype(int))
    len_array = len(col_array) // 500
    for i in range(len_array + 1):
        col_list += (
            session.post(
                f'{BASE_URL}/{request_name}',
                headers=HEADERS_REQ,
                data=(
                    f'fields {fields}; where id = '
                    f'{col_array[i*500:(i+1)*500]}; '
                    'limit 500;'
                )
            )
            .json()
        )
        sleep(0.25)
    try:
        col_list.remove(
            {
                'title': 'Syntax Error',
                'status': 400,
                'cause': (
                    "Expecting a STRING as input, surround your input with "
                    "quotes starting at ')' expecting {'{', 'f', '(', '[', "
                    "'true', 't', 'false', 'null', 'n'"
                )
            }
        )
    except:
        pass
    if give_small:
        return small_df, pd.DataFrame.from_records(col_list)
    return pd.DataFrame.from_records(col_list)


def fuse_small(result, left, group=True, to_dict=False):
    '''
    Funcion utilizada para hacer merge con los resultados de la funcion
    col_of_list
    '''
    if to_dict:
        res_dict = result[1].assign(name=get_dict(result[1]))
    else:
        res_dict = result[1]
    if len(res_dict) == 0:
        return pd.DataFrame(columns=result[0].columns)
    if group:
        return (
            result[0]
            .merge(res_dict, left_on=left, right_on='id', suffixes=('', '_'))
            .drop(['id_', left], axis=1)
            .groupby('id', as_index=False).agg(list)
            .rename(columns={result[1].columns.to_list()[1]: left})
        )
    return (
        result[0]
        .merge(res_dict, left_on=left, right_on='id', suffixes=('', '_'))
        .drop(['id_', left], axis=1)
        .rename(columns={result[1].columns.to_list()[1]: left})
    )


def age_to_dict(cat, rat, desc):
    '''
    Transformacion de los resultados obtenidos del tratamiento de las edades
    recomendadas para un juego
    '''
    if desc == [np.NaN]:
        return {
            'rating': cat + ' ' + str(rat)
        }
    return {
        'rating': cat + ' ' + str(rat),
        'description': desc
    }


def ap_dict_to_list(d_f, col_name, dict_name):
    '''
    Obtencion de resultados con la aplicacion de un diccionario
    '''
    if col_name not in d_f:
        return d_f
    return (
        d_f
        .loc[~d_f[col_name].isnull(), col_name]
        .map(lambda row: [*map(dict_name.get, row)])
    )


def companies_roles(d_f, rol, dict_type):
    '''
    Utilizada para conocer el rol de las distintas desarrolladoras
    involucradas en el desarrollo de un videojuego
    '''
    return (
        d_f
        .loc[d_f[rol]]
        .groupby('id', as_index=False)
        .agg({dict_type: list})
        .rename(columns={dict_type: rol})
    )


def get_dict(d_f):
    '''
    Obtenidos unos datos, se transforman en diccionarios
    '''
    return d_f.apply(
        lambda row: {
            'id': row['id'],
            'name': row[d_f.columns.tolist()[1]]
        },
        axis=1
    )


def dates_to_dict(row):
    '''
    Transformacion de fechas en diccionarios
    '''
    dict_date = []
    for i in range(len(row['release_dates'])):
        dict_date += [{
            'region': row['region'][i],
            'platform': row['platform'][i],
            'date': pd.to_datetime(row['release_dates'][i], unit='s').date()
        }]
    return dict_date


def add_series(row):
    '''
    Fusion de las columnas collection y franchises
    '''
    if isinstance(row['collection'], dict):
        if isinstance(row['franchises'], list):
            if row['collection']['name'] in [
                x['name'] for x in row['franchises']
            ]:
                return row['franchises']
            return row['franchises'] + [row['collection']]
        return [row['collection']]
    return row['franchises']


def add_dlc(row):
    '''
    Fusion de las columnas dlcs y expansions
    '''
    if isinstance(row['dlcs'], list) and isinstance(row['expansions'], list):
        return row['expansions'] + row['dlcs']
    if not isinstance(row['dlcs'], list):
        return row['expansions']
    return row['dlcs']


def get_dates(row):
    '''
    Obtencion de la fecha de la plataforma del juego en cuestion
    '''
    plat_date = [
        {'region': date['region'], 'date': date['date']}
        for date in row['release_dates']
        if date['platform'] == row['platforms']
    ]
    for region in REGION_LIST:
        for date in plat_date:
            if date['region'] == region:
                return date['date']
    return row['first_release_date']


def get_headers(client_id, client_secret):
    '''
    Se obtienen los headers utilizados para conectar con la API
    '''
    global HEADERS_REQ
    access_token = session.post(
        f'https://id.twitch.tv/oauth2/token?client_id={client_id}&'
        f'client_secret={client_secret}&grant_type=client_credentials'
        ).json()['access_token']
    HEADERS_REQ = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {access_token}'
        }
    return HEADERS_REQ


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


def dict_load(headers_dict):
    '''
    Creamos una serie de diccionarios que se usara a lo largo de todo el
    proceso
    '''

    global AGE_DICT
    global AGE_RATING_DICT
    global CAT_DICT
    global MODE_DICT
    global GENRE_DICT
    global PLAT_DICT
    global PERS_DICT
    global THEMES_DICT
    global STATUS_DICT
    global REGION_DICT
    global REGION_LIST

    AGE_DICT = {
        1: 'ESRB',
        2: 'PEGI'
    }

    AGE_RATING_DICT = {
        1: 3,
        2: 7,
        3: 12,
        4: 16,
        5: 18,
        6: 3,
        7: 7,
        8: 7,
        9: 12,
        10: 12,
        11: 16,
        12: 18
    }

    CAT_DICT = {
        0: 'main_game',
        1: 'dlc',
        2: 'dlc',
        3: 'bundle',
        4: 'standalone_expansion',
        6: 'episode',
        7: 'season',
        8: 'remake',
        9: 'remaster',
        10: 'expanded_game',
        11: 'port'
    }

    MODE_DICT = {
        1: 'Single player',
        2: 'Multiplayer',
        3: 'Co-operative',
        4: 'Split screen',
        5: 'Massively Multiplayer Online (MMO)',
        6: 'Battle Royale'
    }

    GENRE_DICT = {
        genre['id']: genre['name'] for genre in (
            session.post(
                f'{BASE_URL}/genres',
                headers=headers_dict,
                data='fields name; limit 500;'
            )
            .json()
        )
    }
    sleep(0.25)

    PLAT_DICT = {
        plat['id']: plat['name'] for plat in (
            session.post(
                f'{BASE_URL}/platforms',
                headers=headers_dict,
                data='fields name; limit 500;'
            )
            .json()
        )
    }
    sleep(0.25)

    PERS_DICT = {
        pers['id']: pers['name'] for pers in (
            session.post(
                f'{BASE_URL}/player_perspectives',
                headers=headers_dict,
                data='fields name; limit 500;'
            )
            .json()
        )
    }
    sleep(0.25)

    THEMES_DICT = {
        theme['id']: theme['name'] for theme in (
            session.post(
                f'{BASE_URL}/themes',
                headers=headers_dict,
                data='fields name; limit 500;'
            )
            .json()
        )
    }
    sleep(0.25)

    STATUS_DICT = {
        0: 'released',
        2: 'alpha',
        3: 'beta',
        4: 'early_access',
        5: 'offline',
        6: 'cancelled',
        7: 'rumored',
        8: 'delisted'
    }

    REGION_DICT = {
        1: 'europe',
        2: 'north_america',
        5: 'japan',
        8: 'worldwide'
        }

    REGION_LIST = [
        'europe',
        'worldwide',
        'north_america',
        'japan'
        ]


# %%
# Definimos una serie de parametros que nos resultaran de utilidad a lo largo
# de esta libreria

BODY_QUERY_FIX = (
    'fields age_ratings, bundles, category, collection, dlcs, '
    'expanded_games, expansions, first_release_date, franchises, '
    'game_engines, game_modes, genres, involved_companies, keywords, name, '
    'parent_game, platforms, player_perspectives, ports, release_dates, '
    'remakes, remasters, standalone_expansions, status, storyline, '
    'summary, themes, updated_at; '
    'sort total_rating_count desc; '
    'where total_rating_count > 1 & category = (0, 4, 8, 9, 10, 11) & '
    'version_parent = null & first_release_date >= '
    )
BASE_URL = 'https://api.igdb.com/v4'

# %%
'''
Definimos las funciones que se usaran en el tratamiento de los datos de cada
uno de los campos a procesar
'''


def get_from_igdb(
        headers_dict,
        start_year=1971,
        end_year=dt.datetime.today().year,
        update=False,
        ):
    '''
    Funcion utilizada para obtener los datos necesarios desde IGDB
    '''
    game_list = []
    for year in range(start_year, end_year+1):
        start_date = int(dt.datetime(year=year, month=1, day=1).timestamp())
        end_date = int(dt.datetime(year=year+1, month=1, day=1).timestamp())
        body_query = BODY_QUERY_FIX + (
            f'{start_date} & first_release_date < {end_date}'
            )
        if update:
            body_query += f' & updated_at > {update}; limit 500;'
        else:
            body_query += '; limit 500;'
        game_list += (
            session.post(
                f'{BASE_URL}/games',
                headers=headers_dict,
                data=body_query
                )
            .json()
            )
        print(f'Juegos de {year} obtenidos de IGDB')
        sleep(0.25)
    return pd.DataFrame(game_list)


def transform_age_ratings(query_df):
    '''
    Funcion utilizada para tratar la columna de age_ratings
    '''
    if 'age_ratings' not in query_df:
        return pd.DataFrame(columns=['id', 'age_ratings'])

    age_cat_df_small, age_cat_df = (
        col_of_list(
            query_df,
            'age_ratings',
            'age_ratings',
            'category, content_descriptions, rating',
            give_small=True
        )
    )

    age_cat_df = (
        age_cat_df
        .loc[lambda df: (df['category'] <= 2) & (df['rating'] <= 12)]
        .assign(
            category=lambda df: df['category'].map(
                lambda row: AGE_DICT[row]
                ),
            rating=lambda df: df['rating'].map(
                lambda row: AGE_RATING_DICT[row]
                )
        )
    )

    age_cd_df = col_of_list(
        age_cat_df,
        'content_descriptions',
        'age_rating_content_descriptions',
        'description'
    )

    age_df = (
        fuse_small(
            (
                age_cat_df_small,
                (
                    age_cat_df
                    .explode('content_descriptions')
                    .merge(
                        age_cd_df,
                        left_on='content_descriptions',
                        right_on='id',
                        suffixes=('', '_')
                    )
                    .drop(['content_descriptions', 'id_'], axis=1)
                    .groupby(['id', 'category', 'rating'], as_index=False)
                    .agg(list)
                    .assign(
                        age_ratings=lambda df: df.apply(
                            lambda row: age_to_dict(
                                row['category'], row['rating'],
                                row['description']
                            ),
                            axis=1
                        )
                    )
                    .drop(['category', 'rating', 'description'], axis=1)
                )
            ),
            'age_ratings')
        .rename(
            columns={'age_ratings_': 'age_ratings'}
        )
    )
    return age_df


def transform_comp(query_df):
    '''
    Funcion utilizada para tratar la columna de involved_companies
    '''
    if 'involved_companies' not in query_df:
        return pd.DataFrame(columns=[
            'id', 'involved_companies', 'developer', 'porting', 'publisher',
            'supporting'
            ])

    comp_df = fuse_small(
        col_of_list(
            query_df,
            'involved_companies',
            'involved_companies',
            'company, developer, porting, publisher, supporting',
            give_small=True
        ),
        'involved_companies',
        group=False
    )

    company_info = col_of_list(
        comp_df,
        'involved_companies',
        'companies',
        'country, name',
        False
    )

    company_info.loc[lambda df: ~df['country'].isnull(), 'country'] = (
        company_info
        .loc[lambda df: ~df['country'].isnull(), 'country']
        .astype(int)
        .map('{:03d}'.format)
        .map(countries.get)
        .str[0]
    )

    comp_df = (
        comp_df
        .merge(
            company_info,
            left_on='involved_companies',
            right_on='id',
            suffixes=('', '_')
        )
        .drop('id_', axis=1)
    )

    comp_df['dict'] = (
        comp_df
        .fillna('')
        .apply(
            lambda row: {
                'company': row['involved_companies'],
                'name': row['name'],
                'country': row['country']
            } if row['country'] != ''
            else {
                'company': row['involved_companies'],
                'name': row['name']
            },
            axis=1)
    )

    comp_df['dict_no_country'] = (
        comp_df
        .fillna('')
        .apply(
            lambda row: {
                'company': row['involved_companies'],
                'name': row['name']
            },
            axis=1
        )
    )
    return comp_df


def transform_dates(query_df):
    '''
    Funcion utilizada para tratar la columna de release_dates
    '''
    if 'release_dates' not in query_df:
        return pd.DataFrame(columns=['id', 'release_dates'])
    dates_df = (
        fuse_small(
            col_of_list(
                query_df,
                'release_dates',
                'release_dates',
                'date, platform, region',
                give_small=True
            ), 'release_dates'
        )
    )

    for col, dict_name in zip(
            ['platform', 'region'], [PLAT_DICT, REGION_DICT]
            ):
        dates_df[col] = ap_dict_to_list(dates_df, col, dict_name)

    dates_df = (
        dates_df
        .assign(release_dates=dates_df.apply(
            lambda x: dates_to_dict(x), axis=1)
            )
        .drop(['platform', 'region'], axis=1)
    )
    return dates_df


def transform_small(query_df):
    '''
    Se transforman una serie de DataFrames que requieren un tratamiento similar
    '''

    df_list = []
    col_name_list = [
        'keywords',
        'game_engines',
        'collection',
        'franchises',
        'ports',
        'remakes',
        'parent_game',
        'remasters',
        'expanded_games',
        'dlcs',
        'standalone_expansions',
        'expansions',
        'bundles'
        ]
    request_name_list = [
        'keywords',
        'game_engines',
        'collections',
        'franchises'
        ]
    request_name_list += ['games']*9
    fields_list = ['name']*13
    for col_name, request_name, fields in zip(
            col_name_list, request_name_list, fields_list
            ):
        if col_name == 'collection':
            explode = False
        else:
            explode = True
        if col_name not in query_df:
            df_list.append(pd.DataFrame(columns=['id', col_name]))
        else:
            df_list.append(
                fuse_small(
                    col_of_list(
                        query_df, col_name, request_name, fields, explode, True
                        ),
                    col_name
                    )
                )
    return df_list


def transform_df(updates_df):
    '''
    Funcion compuesta de todas las transformaciones que sufriran las columnas
    del DataFrame
    '''
    print('Nuevos datos de IGDB obtenidos')
    dict_load(HEADERS_REQ)
    age_df = transform_age_ratings(updates_df)
    print('Age Ratings tratados')
    comp_df = transform_comp(updates_df)
    print('Desarrolladoras tratadas')
    dates_df = transform_dates(updates_df)
    print('Fechas de lanzamiento tratadas')
    small_df_list = transform_small(updates_df)
    print('Columnas sencillas tratadas')
    for d_f in [age_df, dates_df] + small_df_list:
        if d_f.columns[-1] in updates_df:
            updates_df = (
                updates_df
                .drop(d_f.columns[-1], axis=1)
                .merge(d_f, on='id', how='left')
            )
        else:
            updates_df = updates_df.merge(d_f, on='id', how='left')
    print('DataFrames fusionados')
    for col, dict_name in zip(
        ['game_modes', 'genres', 'platforms', 'player_perspectives', 'themes'],
        [MODE_DICT, GENRE_DICT, PLAT_DICT, PERS_DICT, THEMES_DICT]
    ):
        updates_df[col] = ap_dict_to_list(updates_df, col, dict_name)
    print('Diccionarios utilizados')
    updates_df['category'] = updates_df['category'].map(CAT_DICT.get)
    updates_df['first_release_date'] = pd.to_datetime(
        updates_df['first_release_date'], unit='s'
    )
    updates_df['updated_at'] = pd.to_datetime(
        updates_df['updated_at'], unit='s'
        )
    print('Transformaciones de fechas realizadas')
    for col, dict_type in zip(
        comp_df.columns.to_list()[2:6],
        ['dict'] + ['dict_no_country']*3
    ):
        updates_df = updates_df.merge(
            companies_roles(comp_df, col, dict_type), on='id', how='left'
        )
    updates_df.drop('involved_companies', axis=1, inplace=True)
    print('Categorizacion de desarrolladoras realizada')
    updates_tdf = (
        updates_df
        .assign(
            franchises=updates_df.apply(add_series, axis=1),
            expansions=lambda df: df.apply(add_dlc, axis=1)
        )
        .drop(['collection', 'dlcs'], axis=1)
    )
    print('DataFrame de IGDB transformado')
    return updates_tdf


def platform_cleaning(games_df):
    '''
    Limpiamos los juegos de aquellas plataformas que tengan menos de 20 juegos
    '''
    return (
        games_df
        .loc[
            games_df['platforms']
            .isin(
                games_df['platforms']
                .value_counts()
                .loc[lambda val: val >= 20]
                .index
                .tolist()
                )
            ]
        .sort_values(
            ['name', 'first_release_date', 'platforms'],
            ascending=[True, True, True]
        )
        .reset_index(drop=True)
        .astype(str)
        )


def update_igdb(legacy_df, client_id, client_secret):
    '''
    Definimos la funcion que obtendra nuevos resultados para el dataset y
    actualizara los anteriores
    '''
    headers_req = get_headers(client_id, client_secret)
    updates_df = get_from_igdb(
        headers_req,
        update=int(legacy_df['updated_at'].max().timestamp())
        )
    if len(updates_df) == 0:
        print('No hay datos nuevos en IGDB')
        return legacy_df
    updates_tdf = transform_df(updates_df)
    updates_tdf = (
        pd.merge(
            updates_tdf,
            pd.concat(
                [legacy_df[['id']], legacy_df.iloc[:, 30:]], axis=1
            ).drop_duplicates(),
            on='id',
            how='left'
        )
        .explode('platforms')
        .assign(
            release_dates=lambda df: df.apply(get_dates, axis=1)
        )
    )
    new_df = (
        pd.concat([updates_tdf, legacy_df])
        .sort_values('updated_at')
        .drop_duplicates(subset=['id', 'platforms'], keep='last')
        .sort_values(
            ['name', 'first_release_date', 'platforms'],
            ascending=[True, True, True]
        )
        .reset_index(drop=True)
    )
    print('Actualizacion de IGDB completada')
    return new_df
