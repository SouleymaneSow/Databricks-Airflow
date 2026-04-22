# Databricks notebook: 02_silver_transformation.py
# Objectif : nettoyer, typer et préparer les données (Silver)

from pyspark.sql.functions import col, trim

bronze_path = "/Volumes/workspace/default/bronze_auto"
silver_path = "/Volumes/workspace/default/silver_auto"

# -----------------------------
# Lecture Bronze
# -----------------------------
df_bronze = spark.read.format("delta").load(bronze_path)

# -----------------------------
# Nettoyage simple
# -----------------------------
# Exemple : trim des chaînes, suppression des doublons, gestion de quelques nulls
string_cols = [
    f.name for f in df_bronze.schema.fields if str(f.dataType) == "StringType"
]

df_silver = df_bronze

for c in string_cols:
    df_silver = df_silver.withColumn(c, trim(col(c)))

# Suppression des doublons sur NUM_POLICE + Date_debut_police
if "NUM_POLICE" in df_silver.columns and "Date_debut_police" in df_silver.columns:
    df_silver = df_silver.dropDuplicates(["NUM_POLICE", "Date_debut_police"])

# Exemple : filtrer les lignes sans NUM_POLICE ou AGEREV
cols_required = [c for c in ["NUM_POLICE", "AGEREV"] if c in df_silver.columns]
for c in cols_required:
    df_silver = df_silver.filter(col(c).isNotNull())

# -----------------------------
# Écriture Silver
# -----------------------------
(df_silver.write.format("delta").mode("overwrite").save(silver_path))

display(df_silver.limit(20))
