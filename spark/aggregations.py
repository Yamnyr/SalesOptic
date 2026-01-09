from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col, date_trunc

spark = SparkSession.builder.appName("Aggregations").getOrCreate()

# Chiffre d'affaires par produit (existant)
ventes = spark.read.parquet("/data/silver/ventes")
ca_produit = ventes.groupBy("produit") \
    .agg(sum("total").alias("chiffre_affaires"))
ca_produit.write.mode("overwrite").parquet("/data/gold/ca_par_produit")

# Chiffre d'affaires par pays (Join avec Clients)
clients = spark.read.parquet("/data/silver/clients")
ventes_clients = ventes.join(clients, ventes.client_id == clients.id, "inner")
ca_pays = ventes_clients.groupBy("pays") \
    .agg(sum("total").alias("chiffre_affaires"))
ca_pays.write.mode("overwrite").parquet("/data/gold/ca_par_pays")

# Évolution des ventes quotidiennes
ventes_daily = ventes.groupBy(date_trunc("day", col("date")).alias("jour")) \
    .agg(
        sum("total").alias("chiffre_affaires"),
        count("*").alias("nb_ventes")
    ) \
    .orderBy("jour")
ventes_daily.write.mode("overwrite").parquet("/data/gold/ventes_quotidiennes")

# Statistiques des Logs
try:
    logs = spark.read.parquet("/data/silver/logs")
    log_stats = logs.groupBy("level").agg(count("*").alias("count"))
    log_stats.write.mode("overwrite").parquet("/data/gold/log_stats")
except:
    print("Logs non disponibles pour l'agrégation")

spark.stop()
