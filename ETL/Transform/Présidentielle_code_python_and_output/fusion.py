import pandas as pd

# Fonction pour nettoyer et harmoniser les colonnes
def normalize_columns(df, year):
    # Renommer les colonnes pour uniformiser
    df = df.rename(columns={
        "Code du département": "Code_departement",
        "Code de la commune": "Code_commune",
        "Libellé de la commune": "Libelle_commune",
        "Inscrits": "Inscrits",
        "Abstentions": "Abstentions",
        "% Abs/Ins": "Pourcentage_Abstention",
        "Votants": "Votants",
        "% Vot/Ins": "Pourcentage_Votants",
        "Blancs et nuls": "Blancs_et_Nuls",
        "Blancs": "Blancs_et_Nuls",
        "% Blancs/Ins": "Pourcentage_Blancs",
        "% BlNuls/Ins": "Pourcentage_Blancs",
        "% Blancs/Vot": "Pourcentage_Blancs_Votants",
        "% BlNuls/Vot": "Pourcentage_Blancs_Votants",
        "Exprimés": "Exprimes",
        "% Exp/Ins": "Pourcentage_Exprimes",
        "% Exp/Vot": "Pourcentage_Exprimes_Votants",
        "Nom": "Nom",
        "Prénom": "Prenom",
        "Voix": "Voix",
        "Voix_Ins": "Pourcentage_Voix_Ins",
        "Voix_Exp": "Pourcentage_Voix_Exp",
        "% Voix/Ins": "Pourcentage_Voix_Ins",
        "% Voix/Exp": "Pourcentage_Voix_Exp",
        "Nom_Candidat_2": "Nom_Candidat_2",
        "Prenom_Candidat_2": "Prenom_Candidat_2",
        "Voix_Candidat_2": "Voix_Candidat_2",
        "Voix_Ins_Candidat_2": "Pourcentage_Voix_Ins_Candidat_2",
        "Voix_Exp_Candidat_2": "Pourcentage_Voix_Exp_Candidat_2",
        "% Voix/Exp26":"Pourcentage_Voix_Exp_Candidat_2"
    })
    
    # Supprimer les colonnes non nécessaires
    columns_to_drop = ["Code de la circonscription", "Libellé de la circonscription", "Code du b.vote", 
                       "Nuls", "Etat saisie", "N°Panneau", "Libellé du département", "Libelle_departement"]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # Ajouter des colonnes manquantes avec des valeurs NaN
    required_columns = ["Code_departement", "Code_commune", "Libelle_commune",
                        "Inscrits", "Abstentions", "Pourcentage_Abstention", "Votants",
                        "Pourcentage_Votants", "Blancs_et_Nuls", "Pourcentage_Blancs",
                        "Pourcentage_Blancs_Votants", "Exprimes", "Pourcentage_Exprimes",
                        "Pourcentage_Exprimes_Votants", "Nom", "Prenom", "Voix", 
                        "Pourcentage_Voix_Ins", "Pourcentage_Voix_Exp", 
                        "Nom_Candidat_2", "Prenom_Candidat_2", "Voix_Candidat_2", 
                        "Pourcentage_Voix_Ins_Candidat_2", "Pourcentage_Voix_Exp_Candidat_2", "Annee"]
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
    
    # Ajouter une colonne "Annee"
    df["Annee"] = year
    
    # Filtrer uniquement les colonnes requises
    df = df[required_columns]
    
    return df

# Charger les fichiers CSV
df_2007 = pd.read_csv("Output_2007_Néttoyer.csv", delimiter=",")
df_2012 = pd.read_csv("Output_2012_Néttoyer.csv", delimiter=",")
df_2017 = pd.read_csv("Output_2017_Néttoyer.csv", delimiter=",")
df_2022 = pd.read_csv("outpout1_2022.csv", delimiter=",")

# Appliquer le nettoyage et l'harmonisation des colonnes
df_2007 = normalize_columns(df_2007, 2007)
df_2012 = normalize_columns(df_2012, 2012)
df_2017 = normalize_columns(df_2017, 2017)
df_2022 = normalize_columns(df_2022, 2022)

# Afficher les DataFrames dans la console
print("==== Output 2007 ====")
print(df_2007.head())  # Affiche les 5 premières lignes de 2007
print("\n==== Output 2012 ====")
print(df_2012.head())  # Affiche les 5 premières lignes de 2012
print("\n==== Output 2017 ====")
print(df_2017.head())  # Affiche les 5 premières lignes de 2017
print("\n==== Output 2022 ====")
print(df_2022.head())  # Affiche les 5 premières lignes de 2022

# Sauvegarder les DataFrames dans des fichiers CSV
df_2007.to_csv("output2_cleaned_2007.csv", index=False)
df_2012.to_csv("output2_cleaned_2012.csv", index=False)
df_2017.to_csv("output2_cleaned_2017.csv", index=False)
df_2022.to_csv("output2_cleaned_2022.csv", index=False)
