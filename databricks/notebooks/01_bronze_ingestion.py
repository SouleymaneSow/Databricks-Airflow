# Databricks notebook: 01_bronze_ingestion.py
# Objectif : ingérer les données brutes d'assurance auto en Delta (Bronze)

from networkx import display
from pyspark.sql.functions import col, current_timestamp

# -----------------------------
# Paramètres
# -----------------------------

# spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.raw_auto")
# display(dbutils.fs.ls("/Volumes/workspace/default/raw_auto/"))
raw_input_path = "/Volumes/workspace/default/raw_data/Base_de_donnees.csv"  # Unity Catalog + Volume path
bronze_output_path = "/Volumes/workspace/default/bronze_auto"
# -----------------------------
# Lecture des données brutes
# -----------------------------
# Hypothèse : fichier CSV avec header, séparateur ; ou ,
df_raw = (
    spark.read.option("header", True).option("inferSchema", True).csv(raw_input_path)
)

# Ajout de métadonnées techniques
df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp()).withColumn(
    "source_file", col("_metadata.file_path")
)

# -----------------------------
# Écriture en Delta (Bronze)
# -----------------------------
(
    df_bronze.write.format("delta")
    .mode("overwrite")  # pour un POC, overwrite est ok
    .save(bronze_output_path)
)

# Affichage des premières lignes du Bronze
display(df_bronze.limit(20))
