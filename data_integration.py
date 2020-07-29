"""Fichier principal qui exécute toutes les requêtes et
    traitements à la génération des fichiers d'insertion SQL.

"""
import pandas as pd
import requests


from sqlalchemy import create_engine
import pymysql

import config
from DAL.DBConnector import DBConnector


def stations():
    """Insert stations in DB.

    Raises:
        Exception: [description]

    """
    page_initiale = page_stations(0)
    item_per_page = page_initiale.json()['pagination']['items_per_page']
    total_items = page_initiale.json()['pagination']['total_result']
    dfs = []

    # on fait une boucle sur toutes les pages suivantes
    print_done = {}

    for page in range(int(total_items/item_per_page)+1):
        stations_page = page_stations(page)

        ensemble_stations = stations_page.json()

        if 'stop_areas' not in ensemble_stations:
            # pas d'arrêt
            continue

        # on ne retient que les informations qui nous intéressent
        for station in ensemble_stations['stop_areas']:

            station['lat'] = station['coord']['lat']
            station["lon"] = station['coord']['lon']

            if 'administrative_regions' in station.keys():
                for var_api, var_df in zip(
                        ['insee', 'name', 'label', 'id', 'zip_code'],
                        ['insee', 'region', 'label_region', 'id_region',
                            'zip_code']):
                    try:
                        station[var_df] = station['administrative_regions'][0][
                            var_api]
                    except KeyError:
                        if var_api not in print_done:
                            print("key '{0}' not here but {1}".format(
                                var_api, ",".join(station[
                                    'administrative_regions'][0].keys())))
                            print_done[var_api] = var_api

            [station.pop(k, None) for k in [
                'coord', 'links', 'administrative_regions', 'type', 'codes']]

        stations = ensemble_stations['stop_areas']
        try:
            dp = pd.DataFrame(stations)
        except Exception as e:
            # La SNCF modifie parfois le schéma de ses données.
            # On affiche station pour avoir une meilleure idée que l'erreur
            # retournée par pandas
            raise Exception("Problème de données\n{0}".format(stations)) from e

        dfs.append(dp)
        if page % 10 == 0:
            print('Page', page, '---', dp.shape)

    df_stations = pd.concat(dfs)

    del df_stations['label']
    del df_stations['timezone']
    del df_stations['region']
    del df_stations['label_region']
    del df_stations['id_region']
    del df_stations['zip_code']

    # Sanitize
    df_stations['insee'] = df_stations['insee'].apply('{:0>5}'.format)
    # DOM-TOM
    index_dom_tom = df_stations[df_stations['insee'] > '96000'].index
    # Delete these row indexes from dataFrame
    df_stations.drop(index_dom_tom, inplace=True)

    # Delete foreign stations
    df_stations = df_stations[df_stations.insee != '00000']

    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, echo=False)
    df_stations.to_sql('station', con=engine, if_exists='append', index=False)


def page_stations(numero_page):
    """Request SNCF API.

    Args:
        numero_page ([type]): [description]

    Returns:
        [type]: [description]

    """
    return requests.get(
        (f'https://api.sncf.com/v1/coverage/sncf/stop_areas?start_page={numero_page}'),
        auth=(config.TOKEN_AUTH, ''))


def towns():
    """Insert towns in DB.

    Raises:
        Exception: KeyError

    """
    df_towns = pd.read_csv(
        config.CSV_PATH+'towns.csv', encoding='utf-8', usecols=['typecom', 'com', 'libelle']).query('typecom != "COMD"')
    del df_towns['typecom']
    df_towns.columns = ['insee', 'name']

    # DOM-TOM
    index_dom_tom = df_towns[df_towns['insee'] > '96000'].index
    # Delete these row indexes from dataFrame
    df_towns.drop(index_dom_tom, inplace=True)
    # Corsica
    df_towns = df_towns[~df_towns.insee.str.contains('A')]
    df_towns = df_towns[~df_towns.insee.str.contains('B')]

    df_towns['pref'] = False

    # Prefectures
    df_prefs = pd.read_csv(
        config.CSV_PATH+'departments.csv', encoding='utf-8',
        usecols=['cheflieu'])
    # DOM-TOM
    index_dom_tom = df_prefs[df_prefs['cheflieu'] > '96000'].index
    # Delete these row indexes from dataFrame
    df_prefs.drop(index_dom_tom, inplace=True)
    # Corsica
    df_prefs = df_prefs[~df_prefs.cheflieu.str.contains('A')]
    df_prefs = df_prefs[~df_prefs.cheflieu.str.contains('B')]
    # Clf (INSEE: 63113)
    # Station ID : stop_area:OCE:SA:87734004
    df_no_station = pd.DataFrame(columns=['insee'])
    db = DBConnector.getInstance()

    # Clf
    dep = 'stop_area:OCE:SA:87734004'
    datetime = 0

    dep_insee = db.build_select_query(
                    'insee', 'station', 'id', f"'{dep}'")

    df_prefs = df_prefs[df_prefs.cheflieu != dep_insee]
    df_towns.loc[df_towns.insee == dep_insee, 'pref'] = True

    df_journeys = pd.DataFrame(columns=['dep_station_id', 'arr_station_id', 'co2'])
    while not df_prefs.empty:
        df_co2 = pd.DataFrame(columns=['dep', 'arr', 'co2'])
        count = 0
        for pref in df_prefs.itertuples():
            # Only on first loop
            if datetime == 0:
                df_towns.loc[df_towns.insee == pref.cheflieu, 'pref'] = True

            arr = db.build_select_query(
                'id', 'station', 'insee', pref.cheflieu)
            if arr is not None:
                journey = find_journey_info(
                    dep, arr, datetime)
                if journey is not None:
                    count = 0
                    df_co2 = df_co2.append({
                        'dep': dep,
                        'arr': arr,
                        'co2': journey['co2_emission']['value']
                    }, ignore_index=True)
                    datetime = journey['arrival_date_time']
            else:
                df_no_station = df_no_station.append({
                    'insee': pref.cheflieu
                }, ignore_index=True)

        #if not df_co2.empty:
        # Get minimum value of a single column 'co2'
        min = df_co2[df_co2.co2 == df_co2.co2.min()]

        print('Arr : ', arr)
        print('Count : ', count)
        print('Taille df co2 : ', df_co2.shape)

        min = min.iloc[0]['arr']

        min_insee = db.build_select_query(
                    'insee', 'station', 'id', f"'{min}'")

        df_prefs = df_prefs[df_prefs.cheflieu != min_insee]

        print('min ', min)
        db.build_insert_query(
            'journey', '`dep_station_id`, `arr_station_id`',
            f"'{dep}', '{min}'")

        """df_journeys = df_journeys.append({
                    'dep_station_id': dep,
                    'arr_station_id': arr,
                    'co2': journey['co2_emission']['value']
                }, ignore_index=True)"""
        dep = min


    # TODO: N+1, rapide, SaaS
    print(f'df_co2: {df_co2}')
    print(f'df_no_station: {df_no_station}')

    # Df to SQL
    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, echo=False)
    df_towns.to_sql('town', con=engine, if_exists='append', index=False)


def find_journey_info(dep, arr, datetime):
    """Find journey information.

    Args:
        dep (str): departure station
        arr (str): arrival staion
        datetime (str): departure datetime

    Returns:
        JSON object: journey info or None

    """
    if datetime == 0:
        res = requests.get(f'https://api.sncf.com/v1/coverage/sncf/journeys?from={dep}&to={arr}', auth=(config.TOKEN_AUTH, '')).json()
    else:
        res = requests.get(f'https://api.sncf.com/v1/coverage/sncf/journeys?from={dep}&to={arr}&datetime={datetime}', auth=(config.TOKEN_AUTH, '')).json()

    if 'journeys' not in res:
        if 'error' in res and "no solution" in res["error"]['message']:
            print(f'No solution for {dep} --> {arr}.')
            return None

    try:
        return res['journeys'][0]
    except KeyError as err:
        print('KeyError: ', err)
        print('res: ', res)
        print(f'{dep} --> {arr}')
