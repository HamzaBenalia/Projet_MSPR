import pandas as pd

# Recharger le fichier CSV
df = pd.read_csv('grouped_candid.csv')

# Nettoyer et extraire les noms de candidats
def clean_candidate_list(candidate_string):
    # Supprimer les crochets et les guillemets
    candidate_string = candidate_string.strip("[]")
    candidate_string = candidate_string.replace("'", "")
    # Séparer les candidats
    candidates = candidate_string.split(", ")
    return candidates

# Appliquer cette fonction à la colonne 'Candidats'
df['Candidats'] = df['Candidats'].apply(clean_candidate_list)

# Enregistrer le fichier nettoyé dans un nouveau CSV
df.to_csv('cleaned_candidates.csv', index=False)

print("Fichier CSV avec la liste des candidats nettoyée généré.")
