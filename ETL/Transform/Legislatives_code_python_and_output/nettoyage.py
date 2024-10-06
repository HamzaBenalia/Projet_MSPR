from pyspark.sql import SparkSession

# Initialiser Spark
spark = SparkSession.builder.appName("NettoyageColonnes").getOrCreate()

# Charger le fichier CSV
df = spark.read.csv('legislative_2017.csv', header=True, inferSchema=True, sep=',', ignoreLeadingWhiteSpace=True)

# Afficher les colonnes actuelles
print("Colonnes avant suppression :")
print(df.columns)

# Liste des colonnes à garder avec les bons noms
colonnes_a_garder = [
    "Code du département", "Code de la commune", "Libellé de la commune", "Inscrits",
    "Abstentions", "% Abs/Ins", "Votants", "% Vot/Ins", "Blancs", "% Blancs/Ins",
    "% Blancs/Vot", "Exprimés", "% Exp/Ins", "% Exp/Vot", "Nom22", "Prénom23", "Voix25", "% Voix/Ins26", "% Voix/Exp27",
    "Nom30", "Prénom31", "Voix33", "% Voix/Ins34", "% Voix/Exp35"
]

# Sélectionner uniquement les colonnes nécessaires
df_nettoye = df.select(*colonnes_a_garder)

# Afficher les colonnes après suppression
print("Colonnes après suppression :")
print(df_nettoye.columns)

# Sauvegarder dans un nouveau fichier CSV
df_pandas = df_nettoye.toPandas()
df_pandas.to_csv('colonnes_2017.csv', index=False, encoding='utf-8')

# Arrêter Spark
spark.stop()

# Afficher un aperçu des données nettoyées
print("Aperçu des données nettoyées :")
print(df_pandas.head())
