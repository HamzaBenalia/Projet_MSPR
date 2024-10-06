import pandas as pd

# Liste des départements de la région Occitanie
departements_occitanie = [
    "09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"
]

# Charger les fichiers CSV
file_2007 = pd.read_csv("output1_2007_legislative.csv")
file_2012 = pd.read_csv("output1_2012_legislative.csv")
file_2017 = pd.read_csv("output1_2017_legislative.csv")
file_2022 = pd.read_csv("output1_2022_legislative.csv")

# Filtrer les fichiers 2007 et 2012 par les départements de l'Occitanie
file_2007 = file_2007[file_2007["Code_departement"].astype(str).isin(departements_occitanie)]
file_2012 = file_2012[file_2012["Code_departement"].astype(str).isin(departements_occitanie)]

# Normaliser les noms de colonnes pour 2017 car elles diffèrent légèrement
file_2017.columns = [
    "Code_departement", "Code_commune", "Libelle_commune", "Inscrits", "Abstentions", 
    "Pourcentage_Abstention", "Votants", "Pourcentage_Votants", "Blancs_et_Nuls", 
    "Pourcentage_Blancs", "Pourcentage_Blancs_Votants", "Exprimes", "Pourcentage_Exprimes", 
    "Pourcentage_Exprimes_Votants", "Nom_Candidat_1", "Prenom_Candidat_1", "Voix_Candidat_1", 
    "Pourcentage_Voix_Ins_Candidat_1", "Pourcentage_Voix_Exp_Candidat_1", "Nom_Candidat_2", 
    "Prenom_Candidat_2", "Voix_Candidat_2", "Pourcentage_Voix_Ins_Candidat_2", "Pourcentage_Voix_Exp_Candidat_2", 
    "Annee"
]


# Vérifier l'ordre des colonnes et les normaliser
columns_order = [
    "Code_departement", "Code_commune", "Libelle_commune", "Inscrits", "Abstentions", 
    "Pourcentage_Abstention", "Votants", "Pourcentage_Votants", "Blancs_et_Nuls", 
    "Pourcentage_Blancs", "Pourcentage_Blancs_Votants", "Exprimes", "Pourcentage_Exprimes", 
    "Pourcentage_Exprimes_Votants", "Nom_Candidat_1", "Prenom_Candidat_1", "Voix_Candidat_1", 
    "Pourcentage_Voix_Ins_Candidat_1", "Pourcentage_Voix_Exp_Candidat_1", "Nom_Candidat_2", 
    "Prenom_Candidat_2", "Voix_Candidat_2", "Pourcentage_Voix_Ins_Candidat_2", "Pourcentage_Voix_Exp_Candidat_2", 
    "Annee"
]

# Réorganiser les colonnes dans le bon ordre
file_2007 = file_2007[columns_order]
file_2012 = file_2012[columns_order]
file_2017 = file_2017[columns_order]
file_2022 = file_2022[columns_order]

# Fusionner les fichiers
merged_data = pd.concat([file_2007, file_2012, file_2017, file_2022])

# Sauvegarder dans un fichier CSV fusionné
output_file = "fichier_fusionne_occitanie.csv"
merged_data.to_csv(output_file, index=False, encoding='utf-8')

print(f"Fichiers filtrés et fusionnés pour l'Occitanie, sauvegardés dans : {output_file}")
