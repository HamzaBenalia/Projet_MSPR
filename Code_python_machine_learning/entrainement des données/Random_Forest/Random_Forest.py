import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sqlalchemy import create_engine

# Connexion à la base de données PostgreSQL
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
ENDPOINT = 'localhost'  # Remplace par l'URL de ta BDD
USER = 'postgres'
PASSWORD = 'root'
PORT = 5432
DATABASE = 'dbtest'

engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

# Charger les données depuis PostgreSQL
try:
    df = pd.read_sql("SELECT * FROM fact_election", engine)
    print("Données chargées avec succès.")
except Exception as e:
    print(f"Erreur lors du chargement des données: {e}")

# Étape 1 : Analyse de la qualité des données
print("Aperçu des données :")
print(df.head())

# Statistiques descriptives
statistiques_descriptives = df.describe(include='all')
statistiques_descriptives.to_csv('statistiques_descriptives.csv', index=False)
print("Statistiques descriptives enregistrées dans 'statistiques_descriptives.csv'.")

# Vérifier les valeurs manquantes
valeurs_manquantes = df.isnull().sum()
print("Valeurs manquantes par colonne :")
print(valeurs_manquantes)

df = df[(df['Parti_Gagnant'] != 'Inconnu') & (df['Parti_Gagnant'] != 'Extrême-gauche') & (df['Parti_Gagnant'] != 'Égalité')]  # Filtrer les valeurs indésirables

# Remplir les valeurs manquantes avec la médiane pour les colonnes numériques
imputer = SimpleImputer(strategy='median')
df[df.select_dtypes(include=[np.number]).columns] = imputer.fit_transform(df.select_dtypes(include=[np.number]))

# Visualisation de la matrice de corrélation
numerical_df = df.select_dtypes(include=[np.number])
plt.figure(figsize=(10, 8))
sns.heatmap(numerical_df.corr(), annot=True, cmap='coolwarm')
plt.title("Matrice de corrélation")
plt.savefig('correlation_matrix.png')
plt.show()
print("Matrice de corrélation enregistrée dans 'correlation_matrix.png'.")

# Étape 2 : Préparation des données pour l'entraînement
features = numerical_df.columns.drop('Parti_Gagnant', errors='ignore')
X = df[features]
y = df['Parti_Gagnant']

# Normaliser les données
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Séparer les ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.4, random_state=42)

# Étape 3 : Entraîner le modèle
model = RandomForestClassifier(class_weight='balanced', random_state=42, n_estimators=100)  # Utiliser Random Forest
model.fit(X_train, y_train)

# Étape 4 : Évaluer les performances
y_pred = model.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy}")

classification_rep = classification_report(y_test, y_pred, zero_division=0)
print("Classification Report:")
print(classification_rep)

confusion_mat = confusion_matrix(y_test, y_pred)
print("Confusion Matrix:")
print(confusion_mat)

# Enregistrer les performances du modèle
with open('random_forest_performance.txt', 'w') as f:
    f.write(f"Accuracy: {accuracy}\n\n")
    f.write("Classification Report:\n")
    f.write(classification_rep)
    f.write("\nConfusion Matrix:\n")
    f.write(str(confusion_mat))
print("Les performances du modèle ont été enregistrées dans 'random_forest_performance.txt'.")

# Étape 5 : Sauvegarder les prédictions
# Encoder la colonne 'Parti_Gagnant' avec toutes les classes possibles
le_gagnant = LabelEncoder()
le_gagnant.fit(df['Parti_Gagnant'])  # Ajuster le LabelEncoder

# S'assurer que y_test et y_pred sont bien des entiers
y_test = y_test.astype(int)
y_pred = y_pred.astype(int)

# Inverse transform des prédictions
y_test_inverse = le_gagnant.inverse_transform(y_test)
y_pred_inverse = le_gagnant.inverse_transform(y_pred)

# Sauvegarder les prédictions
output_df = pd.DataFrame({
    'Vraie_valeur': y_test_inverse,
    'Prediction': y_pred_inverse
})
output_df.to_csv('predictions_random_forest.csv', index=False)
print("Les prédictions ont été enregistrées dans 'predictions_random_forest.csv'.")
