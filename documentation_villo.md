# 📚 Documentation du Projet : Pipeline de Données Villo (Bruxelles)

## 1. Résumé Exécutif
Ce projet est un système automatisé (un "pipeline de données") conçu pour surveiller, capturer et stocker l'activité du réseau de vélos en libre-service **Villo!** à Bruxelles. L'objectif est de transformer des données éphémères en une véritable mémoire historique (+61 millions de lignes) pour l'analyse décisionnelle.

## 2. Architecture des Données (Medallion)
Les données traversent 3 zones, de la plus brute à la plus raffinée :
* **1. Zone RAW (Bronze) :** Fichiers JSON de l'API stockés tels quels (Sauvegarde absolue).
* **2. Zone STAGING (Silver) :** Zone de travail temporaire. Les JSON sont traduits en tableaux, nettoyés, et préparés pour la fusion.
* **3. Zone ANALYTICS (Gold) :** La vitrine finale optimisée pour l'analyse et la visualisation.

## 3. Modélisation de la Base de Données (Zone Analytics)
Le modèle utilise un Schéma en Étoile :
* **`D_STATION` (Dimension) :** Le catalogue des stations (ID, nom, GPS, capacité). *Mise à jour : 1x/jour.*
* **`F_STATION_STATUS` (Fait) :** Le trafic en temps réel (vélos dispos, bornes vides). *Mise à jour : Toutes les 10 min.*
* **`REF_STATION` (Référence) :** Gestion des attributs administratifs (terminaux bancaires, bonus).

## 4. Structure du Code Python
Le projet est modulaire. Chaque script a une responsabilité unique (principe de la *Single Responsibility*) :
* **`utils.py` :** La boîte à outils. Contient la connexion à la base de données PostgreSQL (`get_pg_conn`) et la logique d'insertion en masse (`batch_insert_staging`) utilisée par tous les autres scripts.
* **Les scripts d'Ingestion (`ingest_*.py`) :** Se connectent à l'API GBFS de Cyclocity, téléchargent les JSON, et les stockent dans la zone RAW avec un timestamp précis (`load_ts`).
* **Les scripts de Transformation (`transform_*.py`) :** Lisent la zone RAW. `transform_villo_station_status.py` gère notamment la logique incrémentale (le "watermark") pour ne traiter que les nouvelles données, calcule les vélos électriques/mécaniques, et insère dans STAGING.
* **`analytics_villo_refresh.py` :** Le chef d'orchestre final. Il contient les requêtes SQL complexes (`UPSERT` pour les dimensions, `INSERT` avec filtre anti-doublon pour les faits) pour déplacer les données de STAGING vers ANALYTICS.

## 5. L'Orchestration (Apache Airflow)
L'automatisation est gérée par deux DAGs distincts pour optimiser les appels API :
* **`dag_villo_dimensions.py` :** Tourne tous les jours à 00h05. Rafraîchit les coordonnées et capacités des stations.
* **`dag_villo_facts.py` :** Tourne toutes les 10 minutes. Capture les variations de trafic.

## 6. Procédure de Déploiement
1. Cloner le dépôt et configurer le `.env` (Identifiants PostgreSQL).
2. Lancer l'infrastructure : `docker-compose up -d`
3. Exécuter les scripts de création des tables SQL (`CREATE TABLE`).
4. (Optionnel) Migrer l'historique : `python scripts_manual/migrate_history.py`
5. Dans Airflow, activer `villo_dimension_daily`, déclencher une exécution, puis activer `villo_facts_10min`.