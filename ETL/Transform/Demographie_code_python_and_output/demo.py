import pandas as pd

# Charger le fichier CSV tout en sautant les 5 premières lignes
file_path = 'demographie_2007.csv'
data = pd.read_csv(file_path, skiprows=5, low_memory=False)  # low_memory=False pour éviter l'avertissement

# Renommer les colonnes
data = data.rename(columns={
    'DEP': 'Code_departement',
    'LIBGEO': 'Libelle_commune',
    'P07_POP': 'Population_commune'
})

# Gérer les valeurs manquantes dans 'Code_commune' et 'Code_departement' avant de convertir en entier
data['Code_departement'] = pd.to_numeric(data['Code_departement'], errors='coerce')

# Remplacer les NaN par des valeurs par défaut (par exemple 0), ou simplement les supprimer si nécessaire
data = data.dropna(subset=['Code_departement'])

# Convertir les colonnes en entier pour supprimer les zéros initiaux
data['Code_departement'] = data['Code_departement'].astype(int)

# Ajouter la colonne Année avec la valeur 2007
data['Annee'] = 2007

# Calculer la population totale par département
population_par_departement = data.groupby('Code_departement')['Population_commune'].sum().reset_index()

# Renommer la colonne de la somme pour plus de clarté
population_par_departement = population_par_departement.rename(columns={'Population_commune': 'Total_population_departement'})

# Joindre cette information au DataFrame d'origine
data_cleaned = pd.merge(data, population_par_departement, on='Code_departement')

# Garder seulement les colonnes nécessaires
data_cleaned = data_cleaned[['Code_departement', 'Libelle_commune', 'Population_commune', 'Annee', 'Total_population_departement']]

# Exporter le fichier nettoyé
output_path = 'chemin_vers_ton_fichier_nettoye.csv'
data_cleaned.to_csv(output_path, index=False)

print(f"Le fichier nettoyé a été exporté sous : {output_path}")
