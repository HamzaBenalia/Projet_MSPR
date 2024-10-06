import pandas as pd

# Charger le fichier CSV
file_path = 'taux de chommage/taux-de-chomage-par-departement-en-occitanie-2007.csv'
data_cleaned = pd.read_csv(file_path, skiprows=3, delimiter=';', encoding='utf-8')

# Colonnes à nettoyer (toutes sauf 'Mois' et 'Total')
columns_to_clean = data_cleaned.columns[1:-1]

# Nettoyer les colonnes en supprimant les espaces et en convertissant en numérique
for column in columns_to_clean:
    data_cleaned[column] = pd.to_numeric(data_cleaned[column].str.replace(' ', ''), errors='coerce')

# Calculer la moyenne par département pour toute l'année
average_unemployment_per_department = data_cleaned[columns_to_clean].mean()

# Exporter les résultats dans un fichier CSV
output_path = 'moyenne_taux_chomage_par_departement_2007.csv'
average_unemployment_per_department.to_csv(output_path, header=True)

# Afficher les résultats
print("Le fichier des moyennes a été exporté sous : ", output_path)
