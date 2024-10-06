import pandas as pd
import os

# Liste des fichiers à fusionner (remplace par les chemins réels de tes fichiers)
fichiers_a_fusionner = [
    'chemin_vers_ton_fichier_nettoye_2007.csv',  # Par exemple : demographie_2007.csv
    'chemin_vers_ton_fichier_nettoye_2012.csv',  # Par exemple : demographie_2008.csv
    'chemin_vers_ton_fichier_nettoye_2017.csv',  # Par exemple : demographie_2009.csv
    'chemin_vers_ton_fichier_nettoye_2022.csv'   # Par exemple : demographie_2010.csv
]

# Charger tous les fichiers dans des DataFrames
dataframes = [pd.read_csv(fichier) for fichier in fichiers_a_fusionner]

# Fusionner tous les DataFrames ensemble
fusion = pd.concat(dataframes, ignore_index=True)

# Définir le nom du fichier de sortie pour la fusion
output_path = 'fichier_fusionne_demo.csv'

# Exporter le fichier fusionné
fusion.to_csv(output_path, index=False)

# Indiquer le chemin du fichier fusionné
print(f"Les fichiers ont été fusionnés et exportés sous : {output_path}")
