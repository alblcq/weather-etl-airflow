import os
import logging
from dotenv import load_dotenv
from datetime import datetime

def setup_logging(log_level="INFO", log_file="weather_etl.log"):
    """
    configure logging for the project
    Logs go to both file and console for visibility
    """
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def get_config():
    """
    Load and validate required environment variables.
    Returns a dictionary with config values
    """
    load_dotenv(dotenv_path="configs/.env")
    cfg = {}
    cfg['OPENWEATHERMAP_API_KEY'] = os.getenv('OPENWEATHERMAP_API_KEY')
    cfg['SNOWFLAKE_USER'] = os.getenv('SNOWFLAKE_USER')
    cfg['SNOWFLAKE_PASSWORD'] = os.getenv('SNOWFLAKE_PASSWORD')
    cfg['SNOWFLAKE_ACCOUNT'] = os.getenv('SNOWFLAKE_ACCOUNT')
    cfg['SNOWFLAKE_DATABASE'] = os.getenv('SNOWFLAKE_DATABASE')
    cfg['SNOWFLAKE_SCHEMA'] = os.getenv('SNOWFLAKE_SCHEMA')
    cfg['SNOWFLAKE_WAREHOUSE'] = os.getenv('SNOWFLAKE_WAREHOUSE')
    cfg['CITIES'] = [c.strip() for c in os.getenv('CITIES', '').split(',')]
    cfg['LOG_LEVEL'] = os.getenv('LOG_LEVEL', 'INFO')
    return cfg

def generate_etl_run_id():
    """
    Creates a unique run ID for tracking
    """
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

