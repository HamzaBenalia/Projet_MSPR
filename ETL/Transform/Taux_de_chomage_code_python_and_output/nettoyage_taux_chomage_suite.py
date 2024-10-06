import pandas as pd

# Charger le fichier correctement
file_path = 'moyenne_taux_chomage_par_departement_2007.csv'
data = pd.read_csv(file_path)

# Renommer les colonnes
data.columns = ['Departement', 'Taux_chomage_moyen']

# Extraire le code de département (en supprimant les noms et les zéros initiaux)
data['Code_departement'] = data['Departement'].str.extract(r'(\d+)').astype(int)

# Réorganiser les colonnes pour avoir 'Code_departement' en premier
data_cleaned = data[['Code_departement', 'Taux_chomage_moyen']]

# Exporter les résultats dans un fichier CSV propre
output_path = 'moyenne_taux_chomage_par_departement_propre.csv'
data_cleaned.to_csv(output_path, index=False)

# Afficher le chemin du fichier exporté
print("Le fichier des moyennes a été exporté sous : ", output_path)
