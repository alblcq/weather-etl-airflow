import pandas as pd
from datetime import datetime

def transform_weather(city_json_pairs, run_id):
    """
    Turn JSON and metadata into a normalized DataFrame
    """
    records = []
    for city, data in city_json_pairs:
        rec = {
            'city_name': city,
            'city_id': data.get('id'),
            'country': data.get('sys', {}).get('country'),
            'latitude': data.get('coord', {}).get('lat'),
            'longitude': data.get('coord', {}).get('lon'),
            'temperature_celsius': data.get('main', {}).get('temp'),
            'feels_like_celsius': data.get('main', {}).get('feels_like'),
            'humidity_percent': data.get('main', {}).get('humidity'),
            'pressure_hpa': data.get('main', {}).get('pressure'),
            'wind_speed_ms': data.get('wind', {}).get('speed'),
            'wind_direction_deg': data.get('wind', {}).get('deg'),
            'cloudiness_percent': data.get('clouds', {}).get('all'),
            'weather_main': data.get('weather', [{}])[0].get('main'),
            'weather_description': data.get('weather', [{}])[0].get('description'),
            'api_timestamp': datetime.fromtimestamp(data.get('dt', 0)),
            'etl_timestamp': datetime.now(),
            'etl_run_id': run_id
        }
        records.append(rec)
    df = pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]
    return df