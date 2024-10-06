import pandas as pd

# Charger les quatre fichiers
df1 = pd.read_csv('output2_cleaned_2007.csv')
df2 = pd.read_csv('output2_cleaned_2012.csv')
df3 = pd.read_csv('output2_cleaned_2017.csv')
df4 = pd.read_csv('output2_cleaned_2022.csv')

# Concaténer les DataFrames les uns en dessous des autres
df_concat = pd.concat([df1, df2, df3, df4], ignore_index=True)

# Trier par la colonne 'Annee'
df_concat_sorted = df_concat.sort_values(by='Annee')

# Afficher le résultat
print(df_concat_sorted.head())

# Sauvegarder le fichier fusionné et trié
df_concat_sorted.to_csv('fichier_concatene3.csv', index=False)

