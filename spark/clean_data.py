from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, length

spark = SparkSession.builder.appName("CleanData").getOrCreate()

# --- Nettoyage des Ventes (CSV) ---
ventes = spark.read.csv("/data/bronze/ventes.csv", header=True, inferSchema=True)

# Définition des critères de qualité
is_valid_vente = (
    col("prix").isNotNull() & (col("prix") > 0) &
    col("quantite").isNotNull() & (col("quantite") > 0) &
    col("produit").isNotNull() & (length(col("produit")) > 0) &
    col("client_id").isNotNull() &
    to_timestamp(col("date")).isNotNull()
)

ventes_clean = ventes.filter(is_valid_vente) \
    .withColumn("total", col("prix") * col("quantite")) \
    .withColumn("date", col("date").cast("date"))

ventes_quarantine = ventes.filter(~is_valid_vente) \
    .withColumn("quarantine_reason", lit("Prix, quantité, produit ou date invalide"))

ventes_clean.write.mode("overwrite").parquet("/data/silver/ventes")
ventes_quarantine.write.mode("append").parquet("/data/quarantine/ventes")

# --- Nettoyage des Événements (JSON) ---
events = spark.read.json("/data/bronze/events.json")

known_events = ["click", "view", "purchase", "add_to_cart", "remove_from_cart"]
is_valid_event = (
    col("user_id").isNotNull() &
    col("event").isin(known_events) &
    to_timestamp(col("timestamp")).isNotNull()
)

events_clean = events.filter(is_valid_event).withColumn("timestamp", to_timestamp(col("timestamp")))
events_quarantine = events.filter(~is_valid_event).withColumn("quarantine_reason", lit("Type d'événement inconnu ou user_id manquant"))

events_clean.write.mode("overwrite").parquet("/data/silver/events")
events_quarantine.write.mode("append").parquet("/data/quarantine/events")

# --- Nettoyage des Clients (Parquet de DB) ---
clients = spark.read.parquet("/data/bronze/clients")
clients.write.mode("overwrite").parquet("/data/silver/clients")

# --- Nettoyage des Logs (Text) ---
try:
    logs = spark.read.text("/data/bronze/logs.txt")
    from pyspark.sql.functions import split, size
    
    # Séparation en colonnes
    logs_parsed = logs.select(
        split(col("value"), ",").getItem(0).alias("timestamp"),
        split(col("value"), ",").getItem(1).alias("level"),
        split(col("value"), ",").getItem(2).alias("service"),
        size(split(col("value"), ",")).alias("parts_count")
    )
    
    is_valid_log = (col("parts_count") == 3) & to_timestamp(col("timestamp")).isNotNull()
    
    logs_clean = logs_parsed.filter(is_valid_log).drop("parts_count").withColumn("timestamp", to_timestamp(col("timestamp")))
    logs_quarantine = logs_parsed.filter(~is_valid_log).drop("parts_count").withColumn("quarantine_reason", lit("Ligne de log malformée"))
    
    logs_clean.write.mode("overwrite").parquet("/data/silver/logs")
    logs_quarantine.write.mode("append").parquet("/data/quarantine/logs")
except Exception as e:
    print(f"Erreur lors du traitement des logs: {e}")

spark.stop()

