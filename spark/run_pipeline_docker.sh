#!/bin/bash
set -e

echo "--------------------------------------------------"
echo "DÉMARRAGE DU PIPELINE SPARK DANS LE CONTENEUR"
echo "--------------------------------------------------"

# Attendre que les données sources soient générées
until [ -f /data/source/ventes.csv ]; do
  echo "En attente de la génération des données sources..."
  sleep 2
done

# Attendre que Postgres soit prêt
echo "Attente de la disponibilité de PostgreSQL..."
# Note: On utilise le nom de service 'postgres' défini dans docker-compose
# On peut utiliser pg_isready si on l'installe, ou simplement boucler sur le job spark-submit

echo "1. Ingestion des données brutes (Bronze)..."
/opt/spark/bin/spark-submit /ingestion/ingest_csv.py
/opt/spark/bin/spark-submit /ingestion/ingest_json.py
/opt/spark/bin/spark-submit /ingestion/ingest_logs.py

echo "2. Ingestion de la base de données (Bronze)..."
# On installe le package postgres au besoin
/opt/spark/bin/spark-submit \
    --packages org.postgresql:postgresql:42.7.1 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /ingestion/ingest_db.py

echo "3. Nettoyage et Traitement (Silver)..."
/opt/spark/bin/spark-submit /spark/clean_data.py

echo "4. Calcul des Agrégations (Gold)..."
/opt/spark/bin/spark-submit /spark/aggregations.py

echo "--------------------------------------------------"
echo "PIPELINE TERMINÉ AVEC SUCCÈS !"
echo "--------------------------------------------------"

# Garder le conteneur en vie si nécessaire, ou on peut le laisser s'arrêter
# Si on veut qu'il reste en vie pour du debug :
# tail -f /dev/null
