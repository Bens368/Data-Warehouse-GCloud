import os
import json
from google.cloud import bigquery
from google.cloud import storage

# Charger les variables d'environnement pour rendre le script adaptable
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your_project_id")
DATASET_NAME = os.getenv("BIGQUERY_DATASET", "your_dataset")
TABLE_NAME = os.getenv("BIGQUERY_TABLE", "your_table")
GCS_TRIGGER_KEYWORD = os.getenv("GCS_TRIGGER_KEYWORD", "stock_data")  # Mot-clé pour filtrer les fichiers

def load_csv_to_bigquery(event, context):
    """
    Fonction pour charger un fichier CSV depuis Google Cloud Storage vers BigQuery.
    Cette fonction est déclenchée automatiquement lorsqu'un fichier est ajouté dans un bucket GCS.
    """
    client = bigquery.Client()
    storage_client = storage.Client()

    # Récupération des informations du fichier déclencheur
    bucket_name = event['bucket']
    file_name = event['name']
    
    # Vérifier que le fichier correspond aux critères définis (mot-clé dans le nom)
    if GCS_TRIGGER_KEYWORD not in file_name:
        print(f"Le fichier {file_name} ne correspond pas aux critères (mot-clé '{GCS_TRIGGER_KEYWORD}' absent).")
        return
    
    uri = f'gs://{bucket_name}/{file_name}'
    table_id = f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}'

    # Configuration du job de chargement
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Ignorer l'en-tête du fichier CSV
        autodetect=True,  # Détection automatique du schéma
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Ajouter les nouvelles données à la table existante
    )
    
    # Exécuter le chargement vers BigQuery
    print(f"Chargement en cours depuis {uri} vers {table_id}...")
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Attendre la fin du job

    print(f"{load_job.output_rows} lignes chargées avec succès dans {table_id}.")

    # Optionnel : Supprimer le fichier après chargement
    # DELETE_AFTER_LOAD = os.getenv("DELETE_AFTER_LOAD", "False").lower() == "true"
    # if DELETE_AFTER_LOAD:
    #     bucket = storage_client.bucket(bucket_name)
    #     blob = bucket.blob(file_name)
    #     blob.delete()
    #     print(f"🗑Fichier {file_name} supprimé du bucket {bucket_name} après chargement.")
