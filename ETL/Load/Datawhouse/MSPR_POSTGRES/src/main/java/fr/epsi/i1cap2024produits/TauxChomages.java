package fr.epsi.i1cap2024produits;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TauxChomages {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DemographyDataCleaning")
                .master("local")
                .getOrCreate();

        // Charger le fichier CSV, en ignorant les 3 premières lignes
        JavaRDD<String> rdd = spark.sparkContext()
                .textFile("C:\\Users\\aymer\\IdeaProjects\\MSPR_CLEN_DATA\\src\\main\\resources\\input\\taux-de-chomage-par-departement-en-occitanie-2007.csv", 1)
                .toJavaRDD();

        // Supprimer les 3 premières lignes
        JavaRDD<String> rddWithoutFirst3Lines = rdd.zipWithIndex()
                .filter(tuple -> tuple._2() > 2) // Ignorer les 3 premières lignes
                .map(Tuple2::_1);

        // Séparer les lignes en colonnes en utilisant le bon délimiteur ";"
        JavaRDD<List<String>> rddSplit = rddWithoutFirst3Lines
                .map(line -> Arrays.asList(line.split(";"))); // Utiliser ";" comme délimiteur

        // Convertir en Dataset<Row> pour utiliser les fonctionnalités Spark SQL
        Dataset<Row> df = spark.createDataset(rddSplit.map(row -> String.join(",", row)).rdd(), Encoders.STRING())
                .selectExpr("split(value, ',') as columns");

        // Extraire les départements depuis la première ligne (avant les valeurs)
        List<String> firstRow = rddSplit.first();  // La première ligne après suppression des 3 lignes
        List<String> departments = firstRow.subList(1, firstRow.size() - 1);  // Extraire les noms des départements

        // Appliquer les noms de colonnes et sélectionner les colonnes "Total" et "Département"
        Dataset<Row> dfWithDepartments = df.withColumn("Department", df.col("columns").getItem(0))  // Première colonne pour les départements
                .selectExpr("columns[" + (firstRow.size() - 1) + "] as Total", "Department");  // Dernière colonne pour le "Total"

        // Afficher les données finales
        dfWithDepartments.show();

        // Enregistrer le DataFrame nettoyé
        String outputPath = "output/cleaned_taux_chomage_data";
        dfWithDepartments.coalesce(1).write().option("header", true).mode("overwrite").csv(outputPath);

        System.out.println("Cleaned data saved to: " + outputPath);

        // Arrêter Spark
        spark.stop();
    }
}
