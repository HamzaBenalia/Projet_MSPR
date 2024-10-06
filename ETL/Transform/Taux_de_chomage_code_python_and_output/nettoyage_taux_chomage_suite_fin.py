import pandas as pd

# Charger le fichier correctement
file_path = 'moyenne_taux_chomage_par_departement_propre.csv'
data = pd.read_csv(file_path)

# Renommer les colonnes
data.columns = ['Departement', 'Taux_chomage_moyen']

# S'assurer que la colonne 'Departement' est bien en format chaîne de caractères
data['Departement'] = data['Departement'].astype(str)

# Extraire le code de département (en supprimant les noms et les zéros initiaux)
data['Code_departement'] = data['Departement'].str.extract(r'(\d+)').astype(int)

# Ajouter la colonne de l'année (2007)
data['Annee'] = 2007

# Réorganiser les colonnes pour avoir 'Code_departement', 'Taux_chomage_moyen', et 'Annee'
data_cleaned = data[['Code_departement', 'Taux_chomage_moyen', 'Annee']]

# Exporter les résultats dans un fichier CSV propre
output_path = 'moyenne_taux_chomage_par_departement_2007_propre.csv'
data_cleaned.to_csv(output_path, index=False)

# Afficher le chemin du fichier exporté
print("Le fichier des moyennes a été exporté sous : ", output_path)
