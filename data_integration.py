"""Fichier principal qui exécute toutes les requêtes et
    traitements à la génération des fichiers d'insertion SQL.

"""
import pandas as pd
import requests


from sqlalchemy import create_engine
import pymysql

import config


def page_stations(numero_page):
    """[summary]

    Args:
        numero_page ([type]): [description]

    Returns:
        [type]: [description]

    """
    return requests.get(
        ('https://api.sncf.com/v1/coverage/sncf/stop_areas?start_page={}').format(numero_page),
        auth=(config.TOKEN_AUTH, ''))


# Init towns df
df_towns = pd.read_csv(config.CSV_PATH+'towns.csv', encoding='utf-8', usecols=[
    'typecom', 'com', 'libelle']).query('typecom != "COMD"')
del df_towns['typecom']
df_towns.columns = ['insee', 'name']

# Init stations df
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
                    station[var_df] = station['administrative_regions'][0][var_api]
                except KeyError:
                    if var_api not in print_done:
                        print("key '{0}' not here but {1}".format(var_api,
                                ",".join(station['administrative_regions'][0].keys())))
                        print_done[var_api] = var_api

        [station.pop(k, None) for k in ['coord', 'links', 'administrative_regions', 'type', 'codes']]

    stations = ensemble_stations['stop_areas']
    try:
        dp = pd.DataFrame(stations)
    except Exception as e:
        # La SNCF modifie parfois le schéma de ses données.
        # On affiche station pour avoir une meilleure idée que l'erreur retournée par pandas
        raise Exception("Problème de données\n{0}".format(stations)) from e

    dfs.append(dp)
    if page % 10 == 0:
        print('Page', page, '---', dp.shape)

df_stations = pd.concat(dfs)
df_stations['insee'] = df_stations['insee'].apply('{:0>5}'.format)

result = pd.merge(
    df_towns, df_stations[['insee', 'lat', 'lon']], on='insee', how='left')


# Df to SQL
engine = create_engine(config.SQLALCHEMY_DATABASE_URI, echo=False)
result.to_sql('town', con=engine, if_exists='append', index=False)
