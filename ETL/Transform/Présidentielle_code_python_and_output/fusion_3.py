import pandas as pd

# Charger le fichier concaténé
df_concat_sorted = pd.read_csv('fichier_concatene3.csv')

# Sélectionner uniquement les colonnes numériques
colonnes_numeriques = ['Inscrits', 'Abstentions', 'Pourcentage_Abstention', 'Votants', 
                       'Pourcentage_Votants', 'Blancs_et_Nuls', 'Pourcentage_Blancs', 
                       'Pourcentage_Blancs_Votants', 'Exprimes', 'Pourcentage_Exprimes', 
                       'Pourcentage_Exprimes_Votants', 'Voix', 'Pourcentage_Voix_Ins', 
                       'Pourcentage_Voix_Exp', 'Voix_Candidat_2', 'Pourcentage_Voix_Ins_Candidat_2', 
                       'Pourcentage_Voix_Exp_Candidat_2']

# Convertir les colonnes numériques au bon type (float)
for col in colonnes_numeriques:
    df_concat_sorted[col] = pd.to_numeric(df_concat_sorted[col], errors='coerce')

# Grouper par commune et année et calculer la moyenne des colonnes numériques
# Conserver les premières valeurs non nulles pour les colonnes non numériques
df_grouped = df_concat_sorted.groupby(['Code_departement', 'Code_commune', 'Libelle_commune', 'Annee']).agg(
    {**{col: 'mean' for col in colonnes_numeriques},  # Calculer la moyenne pour les colonnes numériques
     'Nom': 'first',                                  # Conserver la première occurrence pour les colonnes non numériques
     'Prenom': 'first',
     'Nom_Candidat_2': 'first',
     'Prenom_Candidat_2': 'first'}
).reset_index()

# Réorganiser les colonnes dans l'ordre souhaité
colonnes_ordre = ['Code_departement', 'Code_commune', 'Libelle_commune', 'Annee', 
                  'Inscrits', 'Abstentions', 'Pourcentage_Abstention', 'Votants', 
                  'Pourcentage_Votants', 'Blancs_et_Nuls', 'Pourcentage_Blancs', 
                  'Pourcentage_Blancs_Votants', 'Exprimes', 'Pourcentage_Exprimes', 
                  'Pourcentage_Exprimes_Votants', 'Nom', 'Prenom', 'Voix', 
                  'Pourcentage_Voix_Ins', 'Pourcentage_Voix_Exp', 'Nom_Candidat_2', 
                  'Prenom_Candidat_2', 'Voix_Candidat_2', 'Pourcentage_Voix_Ins_Candidat_2', 
                  'Pourcentage_Voix_Exp_Candidat_2']

df_grouped = df_grouped[colonnes_ordre]

# Afficher le résultat
print(df_grouped.head())

# Sauvegarder le fichier fusionné avec moyennes
df_grouped.to_csv('fichier_grouped2.csv', index=False)
