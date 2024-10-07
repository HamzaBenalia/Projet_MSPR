<img src="Images/download.jpg" alt="Image de Prédiction Électorale" style="width: 100%; height: auto;"/>

# Projet de Prédiction Électorale avec du Machine Learning

Ce projet a été réalisé dans le cadre d'un projet scolaire pour créer une preuve de concept (POC) sur la prédiction des tendances électorales en France. Le projet s'appuie sur des jeux de données historiques d'élections, ainsi que sur des indicateurs socio-économiques tels que le taux de chômage et la démographie.

## Équipe

- **Hamza Benalia**
- **Ayemerick Payet**
- **Nabil Bedjih**
- **Eryan Delmon**

## Objectif du projet

L'objectif de ce projet est de développer un modèle prédictif, basé sur des données historiques, permettant de prédire les résultats des élections futures. La POC se concentre sur un secteur géographique restreint et combine des données d'élections présidentielles et législatives, ainsi que des données sur le chômage et la démographie.

## Jeux de Données Utilisés

Nous avons utilisé plusieurs sources de données publiques pour ce projet :

### Données des élections présidentielles (2007-2022)
### Données des élections législatives (2007-2022)
### Taux de chômage (2007-2017)
### Données démographiques (2007-2022)

Les jeux de données ont été récupérés depuis les plateformes suivantes :

- **[Data.gouv - Élections](https://www.data.gouv.fr/fr/pages/donnees-des-elections/)**
- **[France Travail - Taux de Chômage](https://www.data.gouv.fr/fr/pages/donnees_emploi/)**
- **[INSEE - Institut National de la Statistique et des Études Économiques](https://www.data.gouv.fr/fr/organizations/institut-national-de-la-statistique-et-des-etudes-economiques-insee/)**

## Méthodologie

### 1. Pipeline ETL

- **Extraction (Extract)** : Extraction des jeux de données depuis les sources de données publiques.
- **Transformation (Transform)** : Traitement et nettoyage des données à l'aide de **Apache Spark** et **PySpark** pour garantir leur cohérence et les préparer pour l'analyse.
- **Chargement (Load)** : Les données nettoyées ont été chargées dans une base de données **PostgreSQL** via un schéma en étoile.

### 2. Architecture de Données

Utilisation de **PostgreSQL** avec un schéma en étoile pour organiser les données en tables de faits et dimensions.

### 3. Visualisation des Données

Les données ont été analysées et visualisées à l'aide de **Matplotlib** (Python) et **PowerBI** pour montrer les corrélations entre les résultats électoraux, le chômage, la démographie et d'autres indicateurs socio-économiques.

### 4. Modèles Prédictifs

Nous avons implémenté plusieurs modèles prédictifs avec **Python**, notamment :

- **Régression Linéaire**
- **XGBoost**
- **Random Forest**

Les modèles ont été entraînés sur des données passées et testés pour leur précision. Le modèle **Random Forest** a obtenu une **accuracy** de 94%.

## Outils Utilisés

- **Apache Spark** pour le traitement des données
- **PySpark** pour les opérations de transformation
- **PostgreSQL** pour la gestion de la base de données (Data Warehouse)
- **PowerBI** et **Matplotlib** pour les visualisations
- **Python** pour l'entraînement et l'évaluation des modèles prédictifs (Random Forest, XGBoost, etc.)
- **Google Drive** pour le partage et la collaboration
- **Git** pour le versioning et la collaboration

## Collaboration

Ce projet a été réalisé en utilisant les outils de collaboration suivants :

- **Google Drive** pour le stockage et le partage des fichiers.
- **Git** pour le contrôle de version et la gestion des contributions de l'équipe.

## Comment exécuter le projet

1. Clonez ce dépôt :

   ```bash
   git clone https://github.com/HamzaBenalia/Projet_MSPR.git
   cd Projet_MSPR

2. Pip install -r
   
pandas,
scikit-learn,
xgboost,
random-forest,
apache-spark,
pyspark,
matplotlib

4. python train_model.py


## Conclusion
Ce projet montre la puissance de l'intelligence artificielle pour prédire les résultats d'élections futures en se basant sur des données historiques et des indicateurs socio-économiques. Grâce à des modèles prédictifs avancés comme Random Forest et XGBoost, nous avons pu obtenir des résultats fiables avec une précision de 94%.
