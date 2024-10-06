package fr.epsi.i1cap2024produits.test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class LoadCsvToPostgres {
    public static void main(String[] args) {
        // Configuration de Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("Load CSV to PostgreSQL")
                .setMaster("local[*]");

        // Créer une session Spark
        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Chemin vers le fichier CSV consolidé
        String filePath = "C:\\Users\\hamza benzy\\Desktop\\Datawhouse\\MSPR_POSTGRES\\src\\main\\resources\\input\\fichier_nettoye(2).csv";

        // Charger les données à partir du fichier CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);

        // Correction du typage pour les colonnes avec des valeurs élevées
        df = df.withColumn("Inscrits", df.col("Inscrits").cast(DataTypes.LongType))
                .withColumn("Votants", df.col("Votants").cast(DataTypes.LongType))
                .withColumn("Blancs_et_Nuls", df.col("Blancs_et_Nuls").cast(DataTypes.LongType))
                .withColumn("Exprimes", df.col("Exprimes").cast(DataTypes.LongType))
                .withColumn("Voix", df.col("Voix").cast(DataTypes.LongType))
                .withColumn("Voix_Candidat_2", df.col("Voix_Candidat_2").cast(DataTypes.LongType))
                .withColumn("Total_population_departement", df.col("Total_population_departement").cast(DataTypes.LongType))
                .withColumn("Population_commune", df.col("Population_commune").cast(DataTypes.LongType))
                .withColumn("Pourcentage_Abstention", df.col("Pourcentage_Abstention").cast(DataTypes.DoubleType))
                .withColumn("Pourcentage_Votants", df.col("Pourcentage_Votants").cast(DataTypes.DoubleType))
                .withColumn("Pourcentage_Blancs", df.col("Pourcentage_Blancs").cast(DataTypes.DoubleType))
                .withColumn("Pourcentage_Exprimes", df.col("Pourcentage_Exprimes").cast(DataTypes.DoubleType))
                .withColumn("Pourcentage_Voix_Ins", df.col("Pourcentage_Voix_Ins").cast(DataTypes.DoubleType))
                .withColumn("Pourcentage_Voix_Exp", df.col("Pourcentage_Voix_Exp").cast(DataTypes.DoubleType))
                .withColumn("Pourcentage_Voix_Ins_Candidat_2", df.col("Pourcentage_Voix_Ins_Candidat_2").cast(DataTypes.DoubleType))
                .withColumn("Pourcentage_Voix_Exp_Candidat_2", df.col("Pourcentage_Voix_Exp_Candidat_2").cast(DataTypes.DoubleType));

        // Insertion des données dans les tables de dimensions

        // Dimension Département
        Dataset<Row> dimDepartement = df.select(
                df.col("Code_departement"),
                df.col("Total_population_departement")
        ).distinct(); // Supprime les doublons

        dimDepartement.write()
                .mode("overwrite")
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/dbtest")
                .option("dbtable", "dim_departement")
                .option("user", "postgres")
                .option("password", "root")
                .save();

        // Dimension Commune
        Dataset<Row> dimCommune = df.select(
                df.col("Libelle_commune"),
                df.col("Population_commune")
        ); // Supprime les doublons

        dimCommune.write()
                .mode("overwrite")
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/dbtest")
                .option("dbtable", "dim_commune")
                .option("user", "postgres")
                .option("password", "root")
                .save();

        // Dimension Date
        Dataset<Row> dimDate = df.select(
                df.col("Annee")
        ); // Supprime les doublons

        dimDate.write()
                .mode("overwrite")
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/dbtest")
                .option("dbtable", "dim_date")
                .option("user", "postgres")
                .option("password", "root")
                .save();

        // Dimension Candidat (Candidat 1 et Candidat 2)
        Dataset<Row> dimCandidat = df.select(
                df.col("Nom"),
                df.col("Prenom"),
                df.col("Parti_Politique_Candidat_1"),
                df.col("Nom_Candidat_2"),
                df.col("Prenom_Candidat_2"),
                df.col("Parti_Politique_Candidat_2")
        ); // Supprime les doublons

        dimCandidat.write()
                .mode("overwrite")
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/dbtest")
                .option("dbtable", "dim_candidat")
                .option("user", "postgres")
                .option("password", "root")
                .save();

        // Dimension Type d'élection
        Dataset<Row> dimTypeElection = df.select(
                df.col("type_election"),
                df.col("Parti_Gagnant")
        ); // Supprime les doublons

        dimTypeElection.write()
                .mode("overwrite")
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/dbtest")
                .option("dbtable", "dim_type_election")
                .option("user", "postgres")
                .option("password", "root")
                .save();

        // Table de faits (élections)
        Dataset<Row> dfFiltered = df.select(
                df.col("Libelle_commune"),
                df.col("Code_departement"),
                df.col("Annee"),
                df.col("Inscrits"),
                df.col("Abstentions"),
                df.col("Pourcentage_Abstention"),
                df.col("Votants"),
                df.col("Pourcentage_Votants"),
                df.col("Blancs_et_Nuls"),
                df.col("Pourcentage_Blancs"),
                df.col("Pourcentage_Blancs_Votants"),
                df.col("Exprimes"),
                df.col("Pourcentage_Exprimes"),
                df.col("Pourcentage_Exprimes_Votants"),
                df.col("Voix"),
                df.col("Pourcentage_Voix_Ins"),
                df.col("Pourcentage_Voix_Exp"),
                df.col("Voix_Candidat_2"),
                df.col("Pourcentage_Voix_Ins_Candidat_2"),
                df.col("Pourcentage_Voix_Exp_Candidat_2"),
                df.col("Nom"),
                df.col("Prenom"),
                df.col("Nom_Candidat_2"),
                df.col("Prenom_Candidat_2"),
                df.col("Parti_Politique_Candidat_1"),
                df.col("Parti_Politique_Candidat_2"),
                df.col("Parti_Gagnant"),
                df.col("type_election"),
                df.col("Population_commune"),
                df.col("Total_population_departement"),
                df.col("Taux_chomage_moyen")
        );

        // Informations de connexion à la base de données PostgreSQL
        String jdbcUrl = "jdbc:postgresql://localhost:5432/dbtest";
        String user = "postgres";
        String password = "root";

        // Insérer les données dans la table de faits PostgreSQL
        dfFiltered.write()
                .mode("overwrite")
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "fact_election")
                .option("user", user)
                .option("password", password)
                .save();

        // Vérification des 10 premières lignes
        Dataset<Row> data = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "fact_election")
                .option("user", user)
                .option("password", password)
                .load()
                .limit(10);
        data.show();

        // Arrêter la session Spark
        spark.stop();
    }
}
