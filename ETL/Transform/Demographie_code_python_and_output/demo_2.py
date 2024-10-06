import pandas as pd


# Charger le fichier CSV en spécifiant le délimiteur
file_path = 'demographie_2022.csv'
data = pd.read_csv(file_path, delimiter=';', low_memory=False)  # Remplacer ';' par le bon délimiteur si nécessaire

# Renommer les colonnes
data = data.rename(columns={
    'Code Officiel Département': 'Code_departement',
    'Nom Officiel Commune': 'Libelle_commune',
    'Population totale du dernier exercice': 'Population_commune',
    'Exercice': 'Annee'
})

# Normaliser la colonne 'Code_departement' (supprimer les zéros initiaux)
data['Code_departement'] = pd.to_numeric(data['Code_departement'], errors='coerce').astype(int)

# Calculer la population totale par département
population_par_departement = data.groupby('Code_departement')['Population_commune'].sum().reset_index()

# Renommer la colonne de la somme pour plus de clarté
population_par_departement = population_par_departement.rename(columns={'Population_commune': 'Total_population_departement'})

# Joindre cette information au DataFrame d'origine
data_cleaned = pd.merge(data, population_par_departement, on='Code_departement')

# Garder seulement les colonnes nécessaires
data_cleaned = data_cleaned[['Code_departement', 'Libelle_commune', 'Population_commune', 'Annee', 'Total_population_departement']]

# Exporter le fichier nettoyé
output_path = 'chemin_vers_ton_fichier_nettoye_2022.csv'
data_cleaned.to_csv(output_path, index=False)

print(f"Le fichier nettoyé a été exporté sous : {output_path}")
