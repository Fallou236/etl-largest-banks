import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

code_log = 'code_log.txt'
csv_path = 'exchange_rate.csv'
output_path='Largest_banks_data.csv'


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
sql_connection = sqlite3.connect(db_name)




# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(code_log,"a") as f: 
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            name = cols[1].text.strip()
            mc = cols[2].text.strip().replace(',', '')
            try:
                mc = float(mc)
            except:
                mc = np.nan

            data_dict = {table_attribs[0]: name, table_attribs[1]: mc}
            df1 = pd.DataFrame([data_dict])
            df = pd.concat([df, df1], ignore_index=True)
    log_progress('Extraction des données terminée. Démarrage du processus de transformation')
    return df


def transform(df, csv_path):
    exchange_df = pd.read_csv(csv_path)
    
    # Création  un dictionnaire {devise: taux}
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
    
    # Ajouter des colonnes pour chaque devise, arrondies à 2 décimales
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress('Transformation des données terminée. Démarrage du processus de chargement')
    return df


def load_to_csv(df, output_path):
    # Sauvegarde du DataFrame dans un fichier CSV
    df.to_csv(output_path, index=False)

    # Journalisation
    log_progress("Données enregistrées dans le fichier CSV")


def load_to_db(df, sql_connection, table_name):
    # Sauvegarde du DataFrame dans une BD
    df.to_sql(table_name,sql_connection, if_exists = 'replace', index = 'False')

    # Journalisation
    log_progress('Données chargées dans la base de données sous forme de table, exécution des requêtes')


def run_query(query_statement, sql_connection):
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)


log_progress('Préliminaires terminés. Démarrage du processus ETL')
df = extract(url, table_attribs)
print(df)

df_transformed = transform(df, csv_path)
print(df_transformed)

load_to_csv(df_transformed, output_path)

log_progress('Connexion SQL initiée')

load_to_db(df_transformed, sql_connection, table_name)

run_query(f'SELECT * FROM {table_name}', sql_connection)
run_query(f'SELECT AVG(MC_GBP_Billion) FROM {table_name}', sql_connection)
run_query(f'SELECT Name from {table_name} LIMIT 5', sql_connection)

log_progress('Processus terminé')
