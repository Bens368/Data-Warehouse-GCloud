import os
import json
import pandas as pd
import requests
from google.cloud import bigquery, storage
from datetime import datetime

# Charger les variables d'environnement
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your_project_id")
DATASET_NAME = os.getenv("BIGQUERY_DATASET", "your_dataset")
VIEW_NAME = os.getenv("BIGQUERY_VIEW", "latest_update_view")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "your_bucket")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "your_api_key")
TICKERS = os.getenv("STOCK_TICKERS", "AAPL,NVDA,AMZN").split(",")

# Fonction pour récupérer la dernière date disponible dans BigQuery
def get_latest_date():
    client = bigquery.Client()
    query = f"""
    SELECT latest_date
    FROM `{PROJECT_ID}.{DATASET_NAME}.{VIEW_NAME}`
    WHERE table_name = 'your_table_name'
    """
    query_job = client.query(query)
    results = query_job.result()

    latest_date = None
    for row in results:
        latest_date = row.latest_date

    if not latest_date:
        raise Exception('Erreur : impossible de récupérer la dernière date.')

    return latest_date.strftime('%Y-%m-%d')

# Fonction pour récupérer les données de stock depuis l'API Polygon
def get_stock_data(ticker, start_date, end_date):
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}'
    params = {
        'adjusted': 'true',
        'sort': 'asc',
        'apiKey': POLYGON_API_KEY
    }
    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"Erreur API Polygon pour {ticker} : {response.text}")

    data = response.json()
    if "results" not in data:
        raise Exception(f"Aucune donnée trouvée pour {ticker} entre {start_date} et {end_date}.")

    df = pd.DataFrame(data['results'])
    df['date'] = pd.to_datetime(df['t'], unit='ms')  # Convert timestamp to datetime
    return df

# Fonction pour uploader les données dans Google Cloud Storage
def upload_to_gcs(bucket_name, file_name, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data.to_csv(index=False), content_type='text/csv')
    print(f"Fichier {file_name} téléchargé avec succès dans le bucket {bucket_name}.")

# Fonction principale (exécutée dans un environnement Cloud Function ou localement)
def main(event=None, context=None):
    # Date du jour
    end_date = datetime.now().strftime('%Y-%m-%d')

    # Dernière date connue
    start_date = get_latest_date()

    # Dictionnaire pour stocker les DataFrames
    dataframes = {}

    # Récupérer les données pour chaque ticker
    for ticker in TICKERS:
        df = get_stock_data(ticker, start_date, end_date)[['date', 'vw', 'v', 'n']]
        df.columns = ['date'] + [f"{ticker.lower()}_{col}" for col in df.columns[1:]]
        dataframes[ticker] = df

    # Fusionner les données sur la colonne "date"
    combined_df = dataframes[TICKERS[0]]
    for ticker in TICKERS[1:]:
        combined_df = pd.merge(combined_df, dataframes[ticker], on='date', how='outer')

    # Générer un nom de fichier avec un timestamp
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = f'stock_data_{timestamp}.csv'

    # Upload des données dans Google Cloud Storage
    upload_to_gcs(BUCKET_NAME, file_name, combined_df)

    return 'Données insérées avec succès', 200
