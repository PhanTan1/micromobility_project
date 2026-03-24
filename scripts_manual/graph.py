#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comparatif Prod (Snowflake) vs PoC (Postgres) pour VILLO.
Version Corrigée : Alignement strict des Timezones et Graphiques propres.
"""

import os
import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

try:
    import snowflake.sqlalchemy
except ImportError:
    pass

# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

sns.set_theme(style="whitegrid")
plt.rcParams["figure.dpi"] = 120

DATE_CONFORMITY = os.getenv("DATE_CONFORMITY", "2023-05-15") # Mets une date où tu as de la donnée
DATE_DST = os.getenv("DATE_DST", "2023-10-29") # Date du passage à l'heure d'hiver
OUTPUT_PNG = "audit_comparatif_pipeline.png"

# -------------------------------------------------------------------
# CONNEXIONS
# -------------------------------------------------------------------
def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"), 
        dbname=os.getenv("PG_DATABASE"), user=os.getenv("PG_USER"), password=os.getenv("PG_PASS")
    )

def get_engine_prod():
    url = (f"snowflake://{os.getenv('SNOW_USER')}@{os.getenv('SNOW_ACCOUNT')}/"
           f"{os.getenv('SNOW_DATABASE')}/{os.getenv('SNOW_SCHEMA')}"
           f"?warehouse={os.getenv('SNOW_WAREHOUSE')}&role={os.getenv('SNOW_ROLE')}"
           f"&authenticator={os.getenv('SNOW_AUTHENTICATOR', 'externalbrowser')}")
    return create_engine(url)

# -------------------------------------------------------------------
# REQUÊTES SQL OPTIMISÉES
# -------------------------------------------------------------------
# Snowflake : On garde l'heure locale, on utilise TIME_SLICE
SQL_AGG_10MIN_PROD = f"""
SELECT
  TIME_SLICE("LAST_UPDATE_TS"::TIMESTAMP_NTZ, 10, 'MINUTE') AS ts_10min_local,
  AVG("AVAILABLE_VEHICLES_NB") AS avg_bikes
FROM "PROD_MICROMOBILITY_ANALYTICS"."VILLO"."F_STATION_STATUS"
WHERE "LAST_UPDATE_TS"::DATE = '{DATE_CONFORMITY}'
GROUP BY 1 ORDER BY 1;
"""

# Postgres : On utilise date_bin pour grouper proprement l'UTC
SQL_AGG_10MIN_POC = f"""
SELECT
  date_bin('10 minutes', last_update_ts, TIMESTAMP '2000-01-01') AS ts_10min_utc,
  AVG(available_vehicles_nb) AS avg_bikes
FROM "VILLO_ANALYTICS"."F_STATION_STATUS"
WHERE last_update_ts::date = '{DATE_CONFORMITY}'
GROUP BY 1 ORDER BY 1;
"""

# DST Queries
SQL_DST_PROD = f"""
SELECT EXTRACT(HOUR FROM "LAST_UPDATE_TS"::TIMESTAMP_NTZ) AS hour_local, COUNT(*) AS cnt
FROM "PROD_MICROMOBILITY_ANALYTICS"."VILLO"."F_STATION_STATUS"
WHERE "LAST_UPDATE_TS"::DATE = '{DATE_DST}'
GROUP BY 1 ORDER BY 1;
"""

SQL_DST_POC = f"""
SELECT EXTRACT(HOUR FROM (last_update_ts AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Brussels')::int AS hour_local, COUNT(*) AS cnt
FROM "VILLO_ANALYTICS"."F_STATION_STATUS"
WHERE (last_update_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Brussels')::date = '{DATE_DST}'
GROUP BY 1 ORDER BY 1;
"""

SQL_ORPHANS = f"""
SELECT COUNT(*) FROM "VILLO_ANALYTICS"."F_STATION_STATUS" f
LEFT JOIN "VILLO_ANALYTICS"."D_STATION" d ON f.station_fk = d.station_pk WHERE d.station_pk IS NULL;
"""

# -------------------------------------------------------------------
# GÉNÉRATION DU RAPPORT
# -------------------------------------------------------------------
def generate_report(df_prod, df_poc, df_dst_prod, df_dst_poc, orphans_count):
    logging.info("Génération du rapport visuel...")

    # --- CORRECTION DU BUG TIMEZONE AVANT LE MERGE ---
    # 1. On dit à Pandas que la Prod est en heure de Bruxelles, puis on la convertit en UTC
    df_prod['ts_10min_local'] = pd.to_datetime(df_prod['ts_10min_local'])
    df_prod['ts_utc'] = df_prod['ts_10min_local'].dt.tz_localize('Europe/Brussels', ambiguous='NaT').dt.tz_convert('UTC').dt.tz_localize(None)
    
    # 2. Le PoC est déjà en UTC
    df_poc['ts_utc'] = pd.to_datetime(df_poc['ts_10min_utc'])
    
    # 3. Maintenant on peut fusionner en toute sécurité
    df_merge = pd.merge(df_prod, df_poc, on="ts_utc", how="inner")
    
    # On remet en heure locale pour l'affichage du graphe
    df_merge['Heure Affichage'] = df_merge['ts_utc'].dt.tz_localize('UTC').dt.tz_convert('Europe/Brussels')

    # --- CRÉATION DE LA FIGURE ---
    fig = plt.figure(figsize=(11.69, 16.54))
    gs = fig.add_gridspec(3, 1, hspace=0.35)
    fig.suptitle("Comparatif Prod (Snowflake) vs PoC (Postgres GBFS)", fontsize=20, fontweight='bold', y=0.92)

    # 1) Conformité
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.plot(df_merge['Heure Affichage'], df_merge['avg_bikes_x'], label="Prod (Snowflake)", color="#e74c3c", linewidth=2.5)
    ax1.plot(df_merge['Heure Affichage'], df_merge['avg_bikes_y'], label="PoC (Postgres)", color="#2ecc71", linestyle="--", linewidth=2)
    ax1.set_title(f"1. Conformité — Vélos Disponibles ({DATE_CONFORMITY})", fontsize=14, fontweight='bold')
    ax1.legend()

    # 2) DST (Barres côte à côte avec Seaborn)
    ax2 = fig.add_subplot(gs[1, 0])
    df_dst_prod['Pipeline'] = 'Prod (Snowflake)'
    df_dst_poc['Pipeline'] = 'PoC (Postgres UTC)'
    df_dst_poc = df_dst_poc.rename(columns={"hour_local": "HOUR_LOCAL", "cnt": "CNT"}) # Harmonisation
    df_dst_combined = pd.concat([df_dst_prod, df_dst_poc])
    
    sns.barplot(
    data=df_dst_combined, 
    x="HOUR_LOCAL", 
    y="CNT", 
    hue="Pipeline", 
    hue_order=['Prod (Snowflake)', 'PoC (Postgres UTC)'], # FORCE L'ORDRE ICI
    palette=["#e74c3c", "#2ecc71"], 
    ax=ax2
    )
    ax2.set_title(f"2. Gestion du Changement d'Heure d'Hiver ({DATE_DST})", fontsize=14, fontweight='bold')
    ax2.set_ylabel("Volume de données (lignes)")

    # 3) Scorecard (Qualité)
    ax3 = fig.add_subplot(gs[2, 0])
    df_quality = pd.DataFrame({
        "Métrique": ["Stations Orphelines (Data Integrity)"],
        "Prod (Simulé/Historique)": [14502], # Tu peux mettre ta vraie valeur Snowflake ici
        "PoC (Postgres)": [orphans_count]
    }).melt(id_vars="Métrique", var_name="Pipeline", value_name="Compte")
    
    sns.barplot(
    data=df_quality, 
    x="Métrique", 
    y="Compte", 
    hue="Pipeline", 
    hue_order=['Prod (Simulé/Historique)', 'PoC (Postgres)'], # FORCE L'ORDRE ICI
    palette=["#e74c3c", "#2ecc71"], 
    ax=ax3
    )
    ax3.set_title("3. Qualité des Données (Orphelins)", fontsize=14, fontweight='bold')

    fig.savefig(OUTPUT_PNG, bbox_inches="tight")
    logging.info(f"Rapport sauvegardé : {OUTPUT_PNG}")

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    engine_prod = get_engine_prod()

    # Extraction Snowflake
    df_prod = pd.read_sql(SQL_AGG_10MIN_PROD, engine_prod)
    df_dst_prod = pd.read_sql(SQL_DST_PROD, engine_prod)

    # Extraction Postgres
    with get_pg_conn() as conn:
        df_poc = pd.read_sql(SQL_AGG_10MIN_POC, conn)
        df_dst_poc = pd.read_sql(SQL_DST_POC, conn)
        
        cur = conn.cursor()
        cur.execute(SQL_ORPHANS)
        orphans_count = cur.fetchone()[0]

    if not df_prod.empty and not df_poc.empty:
        generate_report(df_prod, df_poc, df_dst_prod, df_dst_poc, orphans_count)
    else:
        logging.error("Les DataFrames sont vides. Vérifie tes dates dans .env !")

if __name__ == "__main__":
    main()