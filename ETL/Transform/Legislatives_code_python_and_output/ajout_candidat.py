import pandas as pd

# Load the CSV file into a DataFrame
file_path = 'fichier_fusionne.csv'  # Update with your file path
df = pd.read_csv(file_path)

# Grouping by 'Code_commune' or 'Libelle_commune' and listing candidates for each
grouped_candidates = df.groupby(['Libelle_commune', 'Annee']).apply(
    lambda x: pd.Series({
        'Candidats': list(set(x['Nom_Candidat_1'].tolist() + x['Nom_Candidat_2'].tolist()))
    })
).reset_index()

# Show the grouped candidates
print(grouped_candidates)

# Optionally, save the output to a new CSV file
grouped_candidates.to_csv('grouped_candid.csv', index=False, encoding='utf-8')
