from google.cloud import bigquery

# Initialiser le client BigQuery
client = bigquery.Client(project='customer360-migration') 

# Query de test
query = """
    SELECT 
        'BigQuery' as platform,
        'Connected!' as status,
        CURRENT_TIMESTAMP() as timestamp
"""

# Exécuter la query
result = client.query(query).to_dataframe()

# Afficher le résultat
print("\n🎉 SUCCESS! BigQuery connection works!\n")
print(result)
