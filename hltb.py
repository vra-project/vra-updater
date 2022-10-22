'''
Libreria utilizada para la obtencion del dataset procedente de HLTB
'''

# %%

# Cargamos las librerias necesarias para realizar este proceso
import re
import json
from difflib import SequenceMatcher
import datetime as dt
import requests
from requests.adapters import HTTPAdapter, Retry
from user_agent import generate_user_agent
import pandas as pd
from fuzzywuzzy import process
from dateutil.relativedelta import relativedelta
import numpy as np

# %%
# Se definen una serie de funciones que se usaran durante todo el script


def int_to_roman(num):
    '''
    Se define una funcion para transformar numeros en numeros romanos
    '''
    if num.isnumeric():
        num = int(num)
        val = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1]
        syb = [
            'M', 'CM', 'D', 'CD', 'C', 'XC', 'L',
            'XL', 'X', 'IX', 'V', 'IV', 'I'
        ]
        roman_num = ''
        i = 0
        while num > 0:
            for _ in range(num // val[i]):
                roman_num += syb[i]
                num -= val[i]
            i += 1
        return roman_num
    return num


def roman_to_int(name):
    '''
    Se define una funcion para transformar numeros romanos en numeros
    '''
    if re.search(
        r'(IX|IV|V?I{2,3})', name
    ):
        rom_val = {
            'I': 1, 'V': 5, 'X': 10
        }
        int_val = 0
        for i in range(len(name)):
            try:
                if i > 0 and rom_val[name[i]] > rom_val[name[i - 1]]:
                    int_val += rom_val[name[i]] - 2 * rom_val[name[i - 1]]
                else:
                    int_val += rom_val[name[i]]
            except:
                int_val = str(int_val)
                int_val += name[i]
        return str(int_val)
    return name


def get_headers():
    '''
    Se obtienen los headers para realizar las consultas
    '''
    return {
        'content-type': 'application/json',
        'accept': '*/*',
        'User-Agent': generate_user_agent(),
        'referer': REFERER_HEADER
    }


def get_payload(game_title, plat=''):
    '''
    Se obtienen los datos para realizar las consultas
    '''
    return json.dumps(
        {
            'searchType': "games",
            'searchTerms': game_title.split(),
            'searchOptions': {
                'games': {
                    'platform': plat,
                    'rangeTime': {
                        'min': 0,
                        'max': 0
                    }
                }
            }
        }
    )


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
BASE_URL = 'https://howlongtobeat.com/'
REFERER_HEADER = BASE_URL
SEARCH_URL = f'{BASE_URL}api/search'
HLTB_platforms = [
    '3DO', 'Acorn Archimedes', 'Amazon Luna', 'Amiga', 'Amiga CD32',
    'Amstrad CPC', 'Apple II', 'Arcade', 'Atari 2600', 'Atari 5200',
    'Atari 7800', 'Atari 8-bit Family', 'Atari Jaguar', 'Atari Jaguar CD',
    'Atari Lynx', 'Atari ST', 'BBC Micro', 'Browser', 'ColecoVision',
    'Commodore 64', 'Commodore PET', 'Commodore VIC-20', 'Dreamcast',
    'FM Towns', 'Game & Watch', 'Game Boy', 'Game Boy Advance',
    'Game Boy Color', 'Gear VR', 'Gizmondo', 'Google Stadia', 'Intellivision',
    'Interactive Movie', 'Linux', 'Mac', 'Mobile', 'MSX', 'N-Gage',
    'NEC PC-88', 'NEC PC-FX', 'Neo Geo', 'Neo Geo CD', 'Neo Geo Pocket', 'NES',
    'Nintendo 3DS', 'Nintendo 64', 'Nintendo DS', 'Nintendo GameCube',
    'Nintendo Switch', 'Oculus Go', 'Oculus Quest', 'Odyssey', 'Odyssey 2',
    'OnLive', 'Ouya', 'PC', 'Philips CD-i', 'Playdate', 'PlayStation',
    'PlayStation 2', 'PlayStation 3', 'PlayStation 4', 'PlayStation 5',
    'PlayStation Mobile', 'PlayStation Portable', 'PlayStation Vita',
    'Plug & Play', 'Sega 32X', 'Sega CD', 'Sega Game Gear',
    'Sega Master System', 'Sega Mega Drive/Genesis', 'Sega Pico',
    'Sega Saturn', 'SG-1000', 'Sharp X1', 'Sharp X68000', 'Super Nintendo',
    'Tiger Handheld', 'TurboGrafx-16', 'TurboGrafx-CD', 'Virtual Boy', 'Wii',
    'Wii U', 'Windows Phone', 'WonderSwan', 'Xbox', 'Xbox 360', 'Xbox One',
    'Xbox Series X/S', 'Zeebo', 'ZX Spectrum', 'ZX81'
]

# %%
'''
Definimos las funciones que se usaran en el tratamiento de los datos de cada
uno de los campos a procesar
'''


def get_times(game_title, game_id, n_count=1, game_type='', platform=''):
    '''
    Se obtienen los resultados de tiempo para cada juego
    '''
    possible_names = [game_title.strip()]
    if re.search(r'[a-z][A-Z]', game_title):
        erase = re.findall(r'[a-z][A-Z]', game_title)[0]
        possible_names.append(re.sub(
            r'[a-z][A-Z]',
            f'{erase[0]} {erase[1]}',
            game_title
        ))
    possible_names.append(re.sub(r"'", '', game_title).strip())
    possible_names.append(re.sub(r"â", "'", game_title).strip())
    possible_names.append(re.sub('#', '', game_title).strip())
    possible_names.append(re.sub('//', ' ', game_title).strip())
    possible_names.append(re.sub('"', '', game_title).strip())
    possible_names.append(re.sub(' and ', ' & ', game_title).strip())
    possible_names.append(re.sub('The', '', game_title).strip())
    possible_names.append(re.sub(' & ', ' and ', game_title).strip())
    possible_names.append(re.sub(r'–', '', game_title).strip())
    possible_names.append(re.sub(r'​', '', game_title).strip())
    possible_names.append(re.sub(r'[+]', ' plus', game_title).strip())
    possible_names.append(re.sub(
        (
            "Disney's|Disney|HD|Classic|Version|Online|Remastered|Deluxe|"
            "Sid Meier's|Anthology"
            ),
        '',
        game_title
    ).strip())
    possible_names.append(re.sub(r':', '', game_title).strip())
    possible_names.append(' '.join(re.split(r':', game_title)[:-1]).strip())
    if possible_names[-1] in ['Pokémon', 'Star Wars', '']:
        possible_names.remove(possible_names[-1])
    possible_names.append(re.sub(r' - ', ' ', game_title).strip())
    possible_names.append(re.split(r'-', game_title)[0].strip())
    if len(possible_names[-1]) <= 1:
        possible_names.remove(possible_names[-1])
    possible_names.append(re.sub(r'[:-]', '', game_title).strip())
    possible_names = list(dict.fromkeys(possible_names))
    numeric_names = []
    for pos_name in possible_names:
        if re.search(r'\d+', pos_name):
            numeric_names.append(' '.join(
                [int_to_roman(word) for word in pos_name.split()]
            ))
    roman_names = []
    for pos_name in possible_names:
        if re.search(r'(IX|IV|V?I{1,3})', pos_name):
            roman_names.append(' '.join(
                [roman_to_int(word) for word in pos_name.split()]
            ))
    possible_names += numeric_names
    possible_names += roman_names
    possible_names = list(dict.fromkeys(possible_names))
    plat = ''
    if n_count != 1 and game_type in ['remake', 'main_game']:
        plat = PLAT_DICT.get(platform)
    if re.search('Pokémon', game_title):
        plat = PLAT_DICT.get(platform)
    ratios = []
    resp = []
    for game_name in possible_names:
        if len(ratios) > 0:
            if max(ratios) > 0.9:
                break
        try:
            new_resp = (
                session
                .post(
                    SEARCH_URL,
                    headers=get_headers(),
                    data=get_payload(game_name, plat)
                )
                .json()
                ['data']
            )
        except:
            continue
        if len(new_resp) == 0:
            continue
        for game in new_resp:
            ratios.append(SequenceMatcher(
                None,
                game_name.lower(),
                game['game_name'].lower()
            ).ratio())
            if (
                re.search('Pokémon', game['game_name']) and
                re.search('Pokémon', game_name) and
                re.search(
                    ''.join(game_name.split('PokÃ©mon ')),
                    game['game_name']
                ) and
                (
                    re.search('and', game['game_name']) or
                    re.search('Version', game['game_name'])
                )
            ):
                if re.search(
                    game_name.split('Pokémon')[0].strip(), game['game_name']
                ):
                    ratios.remove(ratios[-1])
                    ratios.append(1.0)
            if ratios[-1] > 0.9:
                break
        resp += new_resp
    if len(ratios) > 0:
        result = [game_id]
        if max(ratios) >= 0.9:
            result.append(True)
        else:
            result += [False]
        chosen_game = resp[ratios.index(max(ratios))]
        result.append(chosen_game['game_id'])
        result.append(chosen_game['game_name'])
        result.append(round(chosen_game['comp_main']/3600, 2))
        result.append(round(chosen_game['comp_plus']/3600, 2))
        result.append(round(chosen_game['comp_100']/3600, 2))
        return result
    return [game_id, np.NaN, 0, '', 0, 0, 0]


def get_times_id(game_title, game_id):
    '''
    Se obtienen los resultados de tiempo para cada juego con id conocido
    '''
    resp = (
        session
        .post(
            SEARCH_URL,
            headers=get_headers(),
            data=get_payload(game_title)
        )
        .json()
        ['data']
    )
    chosen_game = [game for game in resp if game['game_id'] == int(game_id)][0]
    result = [game_id]
    result.append(round(chosen_game['comp_main']/3600, 2))
    result.append(round(chosen_game['comp_plus']/3600, 2))
    result.append(round(chosen_game['comp_100']/3600, 2))
    return result


def get_new_times(original_df, date=dt.datetime(year=1970, month=1, day=1)):
    '''
    Obtenemos los nuevos tiempos en base a un DataFrame y una fecha lÃ­mite
    '''
    if date == 'null':
        updated_df = (
            original_df
            .loc[original_df['HLTB_name'] == 'nan']
            .sort_values('release_dates')
            .drop_duplicates('id', keep='first')
            .apply(
                lambda row: get_times_id(row['HLTB_name'], row['HLTB_link']),
                axis=1
            )
            .tolist()
        )
    else:
        updated_df = pd.DataFrame(
            original_df
            .loc[
                (original_df['release_dates'] > date) &
                (original_df['release_dates'] <= dt.datetime.today()) &
                (original_df['HLTB_equal_name'] == 'True')
                ]
            .sort_values('release_dates')
            .drop_duplicates('id', keep='first')
            .apply(
                lambda row: get_times_id(row['HLTB_name'], row['HLTB_link']),
                axis=1
            )
            .tolist(),
            columns=[
                'HLTB_link', 'main_duration', 'extra_duration', 'comp_duration'
            ]
        )

    original_df = column_merge(original_df, updated_df, 'HLTB_link')
    print('Tiempos actualizados')
    return original_df


def prepare_to_time(games_df, update=True):
    '''
    Se obtienen los tiempos de los juegos que estan en el DataFrame de origen
    '''
    timed_df = pd.DataFrame(
        games_df
        .sort_values('release_dates')
        .drop_duplicates('id', keep='first')
        .apply(lambda row:
               get_times(
                   row['name'], row['id'], row['n_count'],
                   row['category'], row['platforms']
               ),
               axis=1
               )
        .tolist(),
        columns=[
            'id', 'HLTB_equal_name', 'HLTB_link', 'HLTB_name',
            'main_duration', 'extra_duration', 'comp_duration'
        ]
    )

    if update:
        games_df = column_merge(games_df, timed_df)
    else:
        games_df = games_df.merge(timed_df, on='id', how='left')

    return games_df


def get_hltb(igdb_df, update=True):
    '''
    Definimos la funcion que obtendra datos desde HLTB
    '''
    game_count = igdb_df.drop_duplicates('id')['name'].value_counts()
    igdb_df = (
        igdb_df
        .assign(
            n_count=igdb_df['name'].map(lambda name: game_count[name])
            )
    )
    if update:
        if len(igdb_df.loc[igdb_df['HLTB_equal_name'].isnull()]) == 0:
            return (
                get_new_times(
                    igdb_df,
                    dt.datetime.today() - relativedelta(months=6)
                    )
                .drop_duplicates(subset=['id', 'platforms'], keep='last')
                .sort_values(
                    ['name', 'first_release_date', 'platforms'],
                    ascending=[True, True, True]
                    )
                .reset_index(drop=True)
                )

    platforms_df = igdb_df['platforms'].unique()
    choices = HLTB_platforms
    platforms_df['close_plat'] = (
        platforms_df['platforms']
        .apply(lambda x: process.extractOne(x, choices)[0])
    )
    # Se revisan resultados y se modifican de forma manual los incorrectos
    platforms_df.loc[
        platforms_df['platforms'].isin([
            '3DO Interactive Multiplayer', 'DOS', 'FM-7',
            'PC (Microsoft Windows)', 'SteamVR', 'Windows Mixed Reality'
            ]),
        'close_plat'
    ] = 'PC'
    platforms_df.loc[
        platforms_df['platforms'].isin([
            'Family Computer', 'Family Computer Disk System',
            'Nintendo Entertainment System'
            ]),
        'close_plat'
    ] = 'NES'
    platforms_df.loc[
        platforms_df['platforms'].isin(['Android', 'BlackBerry OS', 'iOS']),
        'close_plat'
    ] = 'Mobile'
    platforms_df.loc[
        platforms_df['platforms'] == 'PlayStation VR',
        'close_plat'
    ] = 'PlayStation 4'
    platforms_df.loc[
        platforms_df['platforms'].isin(['Satellaview', 'Super Famicom']),
        'close_plat'
    ] = 'Super Nintendo'
    platforms_df.loc[
        platforms_df['platforms'].isin(['Oculus Rift', 'Oculus VR']),
        'close_plat'
    ] = 'Oculus Quest'

    global PLAT_DICT
    PLAT_DICT = platforms_df.set_index('platforms').to_dict()['close_plat']
    print('Relacion de plataformas de HLTB obtenida')
    if update:
        igdb_df = get_new_times(
            igdb_df,
            dt.datetime.today() - relativedelta(months=6)
        )
    if not update:
        hltb_df = prepare_to_time(igdb_df)
    else:
        pre_timed = igdb_df.loc[~igdb_df['HLTB_equal_name'].isnull()]
        post_timed = igdb_df.loc[igdb_df['HLTB_equal_name'].isnull()]
        new_timed_df = prepare_to_time(post_timed)
        hltb_df = (
            pd.concat([pre_timed, new_timed_df])
            .sort_values(['first_release_date', 'release_dates'])
            .drop_duplicates(subset=['id', 'platforms'], keep='last')
            .sort_values(
                ['name', 'first_release_date', 'platforms'],
                ascending=[True, True, True]
                )
            .reset_index(drop=True)
            )
    print('Nuevos tiempos obtenidos')
    return hltb_df
