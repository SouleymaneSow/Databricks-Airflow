# Databricks notebook: 03_gold_aggregation.py
# Objectif : produire des vues agrégées (Gold) pour analyse BI

from networkx import display
from pyspark.sql.functions import col
from pyspark.sql.functions import count as _count
from pyspark.sql.functions import sum as _sum

# ---------------------------------------------
# Paramètres
# ---------------------------------------------
# On suppose que les données Silver sont prêtes et contiennent les colonnes nécessaires
silver_path = "/Volumes/workspace/default/silver_auto"
gold_freq_path = "/Volumes/workspace/default/gold_auto/frequence_par_region"
gold_cost_path = "/Volumes/workspace/default/gold_auto/cout_moyen_par_region"

df_silver = spark.read.format("delta").load(silver_path)

# Hypothèse : colonnes N_SINITRE et montants de sinistres (ex: MONTANT_1, MONTANT_2, etc.)
# On va créer une colonne montant_total_sinistres si possible

montant_cols = [
    c for c in df_silver.columns if c.lower().startswith("montant") or c.isdigit()
]

if montant_cols:
    df_silver = df_silver.withColumn(
        "montant_total_sinistres", sum([col(c) for c in montant_cols])
    )

# ---------------------------------------------
# Vue 1 : fréquence de sinistres par région
# ---------------------------------------------
group_cols = [c for c in ["RÉGION", "REGION"] if c in df_silver.columns]
if not group_cols:
    group_cols = [c for c in ["DEPT", "COMMUNE"] if c in df_silver.columns]

df_freq = (
    df_silver.groupBy(*group_cols)
    .agg(
        _sum(col("N_SINISTRE")).alias("total_sinistres"),
        _count("*").alias("nb_polices"),
    )
    .withColumn("frequence_sinistres", col("total_sinistres") / col("nb_polices"))
)

(df_freq.write.format("delta").mode("overwrite").save(gold_freq_path))

# ---------------------------------------------
# Vue 2 : coût moyen des sinistres par région
# ---------------------------------------------
if "montant_total_sinistres" in df_silver.columns:
    df_cost = (
        df_silver.groupBy(*group_cols)
        .agg(
            _sum(col("montant_total_sinistres")).alias("montant_total"),
            _sum(col("N_SINISTRE")).alias("total_sinistres"),
        )
        .withColumn(
            "cout_moyen_par_sinistre", col("montant_total") / col("total_sinistres")
        )
    )

    (df_cost.write.format("delta").mode("overwrite").save(gold_cost_path))

    display(df_cost.limit(20))

display(df_freq.limit(20))

# Affichage des fichiers dans Gold
display(dbutils.fs.ls("/Volumes/workspace/default/gold_auto"))
