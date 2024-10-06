import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer la session Spark
spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", "/tmp") \
    .appName("FiltrerOccitanie") \
    .getOrCreate()

# Lire le fichier CSV d'entrée avec Spark
input_file = "colonnes-preopre_2017.csv"  # Remplacer par le chemin de votre fichier
df = spark.read.csv(input_file, header=True, inferSchema=True)

# Liste des départements de la région Occitanie
departements_occitanie = ['09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82']

# Filtrer uniquement les départements qui appartiennent à l'Occitanie
df_filtered = df.filter(col("Code du département").isin(departements_occitanie))

# Convertir le DataFrame Spark en Pandas
df_pandas = df_filtered.toPandas()

# Spécifier le chemin de sortie
output_file = "departements_occitanie.csv"  # Remplacer par le chemin de sortie

# Sauvegarder le DataFrame filtré dans un fichier CSV avec Pandas
df_pandas.to_csv(output_file, index=False, encoding='utf-8')

print(f"Fichier filtré sauvegardé sous: {output_file}")
