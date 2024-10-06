import pandas as pd

# Load your dataset
df = pd.read_csv('final_fusion_presidentiel.csv')

# Define the political party for each candidate based on the year and candidate's name
def define_party(row):
    # For Candidate 1
    if row['Annee'] == 2022:
        if row['Nom'] == 'MACRON':
            return 'droite'
        elif row['Nom'] == 'LE PEN':
            return 'extrême-droite'
    elif row['Annee'] == 2017:
        if row['Nom'] == 'MACRON':
            return 'droite'
        elif row['Nom'] == 'LE PEN':
            return 'extrême-droite'
    elif row['Annee'] == 2012:
        if row['Nom'] == 'HOLLANDE':
            return 'gauche'
        elif row['Nom'] == 'SARKOZY':
            return 'droite'
    elif row['Annee'] == 2007:
        if row['Nom'] == 'SARKOZY':
            return 'droite'
        elif row['Nom'] == 'ROYAL':
            return 'gauche'
    return 'inconnu'

def define_party_candidate2(row):
    # For Candidate 2
    if row['Annee'] == 2022:
        if row['Nom_Candidat_2'] == 'MACRON':
            return 'droite'
        elif row['Nom_Candidat_2'] == 'LE PEN':
            return 'extrême-droite'
    elif row['Annee'] == 2017:
        if row['Nom_Candidat_2'] == 'MACRON':
            return 'droite'
        elif row['Nom_Candidat_2'] == 'LE PEN':
            return 'extrême-droite'
    elif row['Annee'] == 2012:
        if row['Nom_Candidat_2'] == 'HOLLANDE':
            return 'gauche'
        elif row['Nom_Candidat_2'] == 'SARKOZY':
            return 'droite'
    elif row['Annee'] == 2007:
        if row['Nom_Candidat_2'] == 'SARKOZY':
            return 'droite'
        elif row['Nom_Candidat_2'] == 'ROYAL':
            return 'gauche'
    return 'inconnu'

# Apply party definition to both candidates
df['Parti_Politique_Candidat_1'] = df.apply(define_party, axis=1)
df['Parti_Politique_Candidat_2'] = df.apply(define_party_candidate2, axis=1)

# Determine the winner based on the votes
def determine_winner(row):
    if row['Voix'] > row['Voix_Candidat_2']:
        return row['Parti_Politique_Candidat_1']
    else:
        return row['Parti_Politique_Candidat_2']

df['Parti_Gagnant'] = df.apply(determine_winner, axis=1)

# Save the updated dataset
df.to_csv('updated_dataset_with_winner.csv', index=False)

print("Dataset updated and saved with political affiliations and winners.")
