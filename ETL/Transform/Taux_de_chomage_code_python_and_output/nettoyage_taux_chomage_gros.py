import pandas as pd
import os

def nettoyer_et_exporter_fichier(file_path, annee):
    try:
        # Lire le fichier avec un délimiteur par défaut et ignorer les premières lignes de métadonnées
        data = pd.read_csv(file_path, delimiter=';', skiprows=3)  # Ajuste le skiprows si nécessaire

        # Transposer les données pour avoir une colonne par département
        data_transposed = data.melt(id_vars=['Mois'], var_name='Departement', value_name='Taux_chomage_moyen')

        # Nettoyer la colonne 'Departement' pour extraire le code du département
        data_transposed['Code_departement'] = data_transposed['Departement'].str.extract(r'(\d+)')

        # Gérer les valeurs manquantes avant de convertir en entier
        data_transposed['Code_departement'] = pd.to_numeric(data_transposed['Code_departement'], errors='coerce')

        # Supprimer les lignes où le code du département est manquant (NaN)
        data_transposed = data_transposed.dropna(subset=['Code_departement'])

        # Convertir les codes en entiers (sans décimales)
        data_transposed['Code_departement'] = data_transposed['Code_departement'].astype(int)

        # Nettoyer les valeurs dans 'Taux_chomage_moyen' en supprimant les espaces, etc.
        data_transposed['Taux_chomage_moyen'] = data_transposed['Taux_chomage_moyen'].astype(str).str.replace(' ', '')
        data_transposed['Taux_chomage_moyen'] = pd.to_numeric(data_transposed['Taux_chomage_moyen'], errors='coerce')

        # Calculer la moyenne du taux de chômage par département
        moyenne_par_departement = data_transposed.groupby('Code_departement')['Taux_chomage_moyen'].mean().reset_index()

        # Ajouter la colonne de l'année
        moyenne_par_departement['Annee'] = annee

        # Réorganiser les colonnes pour avoir 'Code_departement', 'Taux_chomage_moyen', et 'Annee'
        data_cleaned = moyenne_par_departement[['Code_departement', 'Taux_chomage_moyen', 'Annee']]

        # Définir le nom du fichier de sortie
        output_file_name = f'moyenne_taux_chomage_par_departement_{annee}.csv'
        output_path = os.path.join(os.path.dirname(file_path), output_file_name)

        # Exporter les résultats dans un fichier CSV propre
        data_cleaned.to_csv(output_path, index=False)

        # Retourner le chemin du fichier exporté
        print(f"Le fichier des moyennes pour {annee} a été exporté sous : {output_path}")

    except pd.errors.ParserError as e:
        print(f"Erreur lors de la lecture du fichier {file_path}: {e}")
    except Exception as e:
        print(f"Une autre erreur est survenue pour le fichier {file_path}: {e}")

# Liste des fichiers à traiter avec les années correspondantes
fichiers_et_annees = [
    ('taux de chommage/taux-de-chomage-par-departement-en-occitanie-2012.csv', 2012),
    ('taux de chommage/taux-de-chomage-par-departement-en-occitanie-2017.csv', 2017),
    ('taux de chommage/taux-de-chomage-par-departement-en-occitanie-2022.csv', 2022)
]

# Traiter chaque fichier
for fichier, annee in fichiers_et_annees:
    nettoyer_et_exporter_fichier(fichier, annee)
