import os
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration mapping from .env
DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = os.getenv("PG_PORT", "5432")
DB_NAME = os.getenv("PG_DATABASE", "stage_micromobility")
DB_USER = os.getenv("PG_USER", "tan")
DB_PASS = os.getenv("PG_PASS")

def get_connection(dbname=None):
    """Utility function to create a flexible database connection"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=dbname or DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def create_database():
    """Create the target database if it does not exist"""
    # Connect to default 'postgres' database to execute creation
    conn = get_connection("postgres") 
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        cur.execute(f'CREATE DATABASE "{DB_NAME}"')
        print(f"Database '{DB_NAME}' created successfully.")
    except errors.DuplicateDatabase:
        print(f"Database '{DB_NAME}' already exists.")
    except Exception as e:
        print(f"Error during database creation: {e}")
    finally:
        cur.close()
        conn.close()

def setup_tables():
    """Create schemas and tables with session verification"""
    
    # Phase 1: Schema Creation
    conn = get_connection(DB_NAME)
    conn.autocommit = True 
    cur = conn.cursor()
    
    try:
        print(f"Connected to {DB_NAME}. Creating schemas...")
        cur.execute('CREATE SCHEMA IF NOT EXISTS "VILLO_RAW";')
        cur.execute('CREATE SCHEMA IF NOT EXISTS "VILLO_STAGING";')
        cur.execute('CREATE SCHEMA IF NOT EXISTS "VILLO_ANALYTICS";')
        print("Schemas initialized: VILLO_RAW, VILLO_STAGING, VILLO_ANALYTICS.")
    except Exception as e:
        print(f"Error during schema creation: {e}")
        return
    finally:
        cur.close()
        conn.close()

    # Phase 2: Table Creation (New session to ensure schema visibility)
    conn = get_connection(DB_NAME)
    conn.autocommit = True
    cur = conn.cursor()

    table_commands = [
        # Extension needed for gen_random_uuid()
        'CREATE EXTENSION IF NOT EXISTS "pgcrypto";',
        # VILLO_RAW Layer
        'CREATE TABLE IF NOT EXISTS "VILLO_RAW"."D_STATION" (raw jsonb, source_url varchar(300), load_ts timestamp DEFAULT CURRENT_TIMESTAMP);',
        'CREATE TABLE IF NOT EXISTS "VILLO_RAW"."F_STATION_STATUS" (raw jsonb, source_url varchar(300), load_ts timestamp DEFAULT CURRENT_TIMESTAMP);',
        
        # VILLO_STAGING Layer
        """
        CREATE TABLE IF NOT EXISTS "VILLO_STAGING"."D_STATION" (
            station_id varchar(80), name_en varchar(200), name_nl varchar(200), name_fr varchar(200),
            lat double precision, lon double precision, address varchar(500), capacity integer,
            load_ts timestamp DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS "VILLO_STAGING"."F_STATION_STATUS" (
            station_id text, last_updated timestamp, last_reported timestamp,
            num_vehicles_available integer, num_docks_available integer,
            num_vehicles_disabled integer, num_docks_disabled integer,
            vehicle_types_available jsonb, mechanical_count integer,
            electrical_count integer, is_installed boolean, is_renting boolean,
            is_returning boolean, load_ts timestamp
        );
        """,
        
        # VILLO_ANALYTICS 
        """
        CREATE TABLE IF NOT EXISTS "VILLO_ANALYTICS"."D_STATION" (
            station_id varchar(80), 
            station_pk bigint, 
            station_name text, 
            archipel text,
            gid bigint, 
            name_fr varchar(200), 
            name_nl varchar(200), 
            address_fr varchar(500), 
            address_nl varchar(500),
            postal_cd integer, 
            commune_fr varchar(100),
            commune_nl varchar(100), 
            lat double precision, 
            lon double precision, 
            load_ts timestamp
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS "VILLO_ANALYTICS"."F_STATION_STATUS" (
            station_status_pk UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            station_fk bigint, 
            last_update_ts timestamp,
            stands_nb bigint, 
            available_stands_nb bigint, 
            available_vehicles_nb bigint,
            status varchar(10), 
            bonus_flag boolean, 
            banking_flag boolean, 
            load_ts timestamp
        );
        """
    ]

    try:
        print("Creating tables...")
        for command in table_commands:
            cur.execute(command)
        print("All tables created successfully.")
    except Exception as e:
        print(f"Error during table creation: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print(f"Initializing connection to {DB_HOST}:{DB_PORT} as user {DB_USER}...")
    create_database()
    setup_tables()