import pandas as pd
import os

# Liste des fichiers à fusionner (remplace par les chemins réels de tes fichiers)
fichiers_a_fusionner = [
    'moyenne_taux_chomage_par_departement_2007.csv',  # Par exemple : moyenne_taux_chomage_par_departement_2012.csv
    'taux de chommage/moyenne_taux_chomage_par_departement_2012.csv',  # Par exemple : moyenne_taux_chomage_par_departement_2017.csv
    'taux de chommage/moyenne_taux_chomage_par_departement_2017.csv',  # Par exemple : moyenne_taux_chomage_par_departement_2022.csv
    'taux de chommage/moyenne_taux_chomage_par_departement_2022.csv'   # Par exemple : moyenne_taux_chomage_par_departement_2023.csv
]

# Charger tous les fichiers dans des DataFrames et les fusionner
dataframes = [pd.read_csv(fichier) for fichier in fichiers_a_fusionner]

# Fusionner tous les DataFrames ensemble
fusion = pd.concat(dataframes, ignore_index=True)

# Définir le nom du fichier de sortie pour la fusion
output_path = 'fichier_fusionne.csv'

# Exporter le fichier fusionné
fusion.to_csv(output_path, index=False)

# Indiquer le chemin du fichier fusionné
print(f"Les fichiers ont été fusionnés et exportés sous : {output_path}")
