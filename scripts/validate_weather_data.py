import snowflake.connector
import logging
from scripts.utils import get_config

def validate_weather_data(etl_run_id=None):
    cfg = get_config()
    conn = snowflake.connector.connect(
        user=cfg['SNOWFLAKE_USER'],
        password=cfg['SNOWFLAKE_PASSWORD'],
        account=cfg['SNOWFLAKE_ACCOUNT'],
        warehouse=cfg['SNOWFLAKE_WAREHOUSE'],
        database=cfg['SNOWFLAKE_DATABASE'],
        schema='TRANSFORMED_DATA'
    )
    cur = conn.cursor()
    # Data quality
    query = """
        SELECT *
        FROM WEATHER_TRANSFORMED
        WHERE (CITY_NAME IS NULL
            OR API_TIMESTAMP IS NULL
            OR TEMPERATURE_CELSIUS NOT BETWEEN -80 AND 60
            OR HUMIDITY_PERCENT NOT BETWEEN 0 AND 100)
            AND ETL_RUN_ID = %s
    """
    params = (etl_run_id,) if etl_run_id else ()
    cur.execute(query, params)
    bad_rows = cur.fetchall()
    if bad_rows:
        logging.warning(f"Data quality check failed: {len(bad_rows)} issues found.")
        for row in bad_rows:
            logging.warning(f"Bad Row: {row}")
    else:
        logging.info("Data quality check passed: no issues found.")
    conn.close()