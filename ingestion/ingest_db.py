from pyspark.sql import SparkSession
import time
import sys

print("Initialisation de Spark...")
spark = SparkSession.builder \
    .appName("IngestDB") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .config("spark.jars.ivy", "/tmp/.ivy2") \
    .getOrCreate()

print("Tentative de connexion à PostgreSQL...")
print("URL: jdbc:postgresql://postgres:5432/bigdata")

try:
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/bigdata") \
        .option("dbtable", "clients") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    print(f"Connexion réussie ! {df.count()} lignes récupérées")
    print("Aperçu des données:")
    df.show()
    
    print("Écriture en Parquet...")
    df.write.mode("overwrite").parquet("/data/bronze/clients")
    print("Données écrites avec succès dans /data/bronze/clients")
    
except Exception as e:
    print(f"Erreur lors de l'ingestion: {str(e)}")
    print(f"Type d'erreur: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    spark.stop()
    sys.exit(1)

spark.stop()
print("Ingestion PostgreSQL terminée avec succès")
