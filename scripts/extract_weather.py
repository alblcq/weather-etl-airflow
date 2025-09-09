import requests
import logging
from scripts.utils import get_config, setup_logging, generate_etl_run_id
import sys
import pandas as pd

def extract_weather_data(cities, api_key):
    """
    Extract weather data for a list of cities from OpenWeatherMap API
    Returns a list of city and api_json
    """
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
    data = []
    for city in cities:
        try:
            params = {
                "q": city.strip(),
                "appid": api_key,
                "units": "metric"
            }
            resp = requests.get(BASE_URL, params=params, timeout=10)
            resp.raise_for_status()
            api_json = resp.json()
            data.append((city, api_json))
            logging.info(f"Success: Extracted weather for {city}")
        except Exception as e:
            logging.error(f"Error for city {city}: {e}")
    return data

def main():
    from scripts.load_snowflake import load_raw_to_snowflake, load_transformed_to_snowflake
    from scripts.transform_weather import transform_weather
    from scripts.validate_weather_data import validate_weather_data
    from scripts.utils import setup_logging, get_config, generate_etl_run_id

    setup_logging()
    cfg = get_config()
    cities = cfg["CITIES"]
    api_key = cfg["OPENWEATHERMAP_API_KEY"]
    run_id = generate_etl_run_id()
    city_json_pairs = extract_weather_data(cities, api_key)
    load_raw_to_snowflake(city_json_pairs, run_id)
    df = transform_weather(city_json_pairs, run_id)
    load_transformed_to_snowflake(df)
    validate_weather_data(run_id)

if __name__ == "__main__":
    main()