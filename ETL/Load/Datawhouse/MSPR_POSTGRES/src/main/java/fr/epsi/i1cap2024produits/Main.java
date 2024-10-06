package fr.epsi.i1cap2024produits;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DemographyDataCleaning")
                .master("local")  // Run in local mode
                .getOrCreate();

        // Load the CSV file into a DataFrame
        Dataset<Row> df = spark.read().option("header", true)
                .option("delimiter", ";")  // Adjust if your CSV uses a different delimiter
                .csv("C:\\Users\\aymer\\IdeaProjects\\MSPR_CLEN_DATA\\src\\main\\resources\\input\\populations-ofgl-communes-2022.csv");  // Ensure this file is in the correct path

        // Show first few rows of the raw DataFrame to verify data is loaded
        df.show(10);  // Show the first 10 rows for debugging

        // Show the schema to ensure the columns are being read correctly
        df.printSchema();

        // Check how many rows were read
        System.out.println("Total Rows before cleaning: " + df.count());

        // Keep only the relevant columns, rename "Exercice" to "Année", and remove the "siren" column
        Dataset<Row> dfCleaned = df.select("Exercice", "Code commune", "Nom Collectivité", "Population municipale",
                        "Population totale", "Tranche revenu par habitant", "Commune rurale",
                        "Nom Officiel Commune", "Nom Officiel Région", "Nom Officiel Département")
                .withColumnRenamed("Exercice", "Année");  // Rename the column

        // Show the selected columns to verify the selection
        dfCleaned.show(10);

        // Filter out rows with missing data in important columns
        dfCleaned = dfCleaned.na().drop("any", new String[]{"Code commune", "Population municipale", "Population totale"});

        // Show the cleaned data
        dfCleaned.show(10);

        // Check how many rows remain after filtering
        System.out.println("Total Rows after cleaning: " + dfCleaned.count());

        // Specify the output path for the cleaned data
        String outputPath = "output/cleaned_demography_data";

        // Write the cleaned data back to a CSV file
        dfCleaned.coalesce(1).write().option("header", true)
                .mode("overwrite")
                .csv(outputPath);

        System.out.println("Cleaned data saved to: " + outputPath);

        // Stop Spark
        spark.stop();
    }
}