import snowflake.connector
import logging
import json
from datetime import datetime
from scripts.utils import get_config
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

def load_raw_to_snowflake(city_json_pairs, run_id):
    """
    Loads raw weather API responses into Snowflake
    """
    cfg = get_config()
    conn = None
    try: 
        conn = snowflake.connector.connect(
            user=cfg['SNOWFLAKE_USER'],
            password=cfg['SNOWFLAKE_PASSWORD'],
            account=cfg['SNOWFLAKE_ACCOUNT'],
            warehouse=cfg['SNOWFLAKE_WAREHOUSE'],
            database=cfg['SNOWFLAKE_DATABASE'],
            schema=cfg['SNOWFLAKE_SCHEMA']
        )
        cur = conn.cursor()
        query = """
            INSERT INTO weather_raw (
                city_name, raw_response, api_timestamp, etl_timestamp, etl_run_id
            )
            SELECT %s, PARSE_JSON(%s), %s, CURRENT_TIMESTAMP(), %s
        """
        for city, api_json in city_json_pairs:
            # Timestamp from API or now if missing
            api_timestamp = datetime.fromtimestamp(api_json.get("dt", datetime.now().timestamp()))
            cur.execute(
                query,
                (city, json.dumps(api_json), api_timestamp, run_id)
            )
            logging.info(f"Inserted raw data for {city}")
        conn.commit()
    except Exception as e:
        logging.error(f"Snowflake load error: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# Loading clean data to snowflake
def load_transformed_to_snowflake(df):
    cfg = get_config()
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=cfg['SNOWFLAKE_USER'],
            password=cfg['SNOWFLAKE_PASSWORD'],
            account=cfg['SNOWFLAKE_ACCOUNT'],
            warehouse=cfg['SNOWFLAKE_WAREHOUSE'],
            database=cfg['SNOWFLAKE_DATABASE'],
            schema='TRANSFORMED_DATA'
        )
        # Weather_transformed table
        #Fixing date columns
        df['API_TIMESTAMP'] = df['API_TIMESTAMP'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        df['ETL_TIMESTAMP'] = df['ETL_TIMESTAMP'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        success, nchunks, nrows, _ = write_pandas(conn, df, 'WEATHER_TRANSFORMED')
        print(f"Inserted {nrows} rows in {nchunks} chunk(s), success: {success}")
    except Exception as e:
        logging.error(f"Error Loading Dataframe to Snowflake: {e}", exc_info=True)
    finally:
        if 'conn' in locals():
            conn.close()