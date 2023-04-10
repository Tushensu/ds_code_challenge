import boto3
import logging
import configparser
import os
import pandas as pd
import geopandas as gpd
from geopy.point import Point
from geopy.distance import distance
from geopy.geocoders import Nominatim
from pathlib import Path
from src.utils.constants import BUCKET_NAME, H3_POLYGONS_LVL_8_9_10, H3_POLYGONS_LVL_8, SERVICE_REQUEST_DATA, WIND_DATA
from src.utils.extract_data import get_city_geojson, get_sr_data
from src.utils.validation import validate_data_extract
from src.utils.helper_functions import get_location_centroid
from src.utils.transformations import join_sr_to_gpd_data_extract, filter_sr_data_by_distance, get_wind_data, clean_wind_data, merge_wind_data
from src.utils.helper_functions import benchmark


# Set paths
HOME_DIR = Path.cwd()
DATA_DIR = HOME_DIR / 'data'
LOGS_DIR = HOME_DIR / 'logs'
CONFIG_DIR = HOME_DIR / 'src' / 'config'

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=LOGS_DIR / 'cpt_ds_challenge.log'
                    )


# Get AWS keys
config = configparser.ConfigParser()
config.read_file(open(CONFIG_DIR / 'dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get(
    'AWS', 'AWS_SECRET_ACCESS_KEY')


@benchmark
def main():
    """
    This is the main function that runs the entire program
    """

    # Set up S3 client
    s3_client = boto3.client('s3', region_name='af-south-1')

    # Fetch H3 resolution 8 data from S3
    city_polygons_extract = f"""
        SELECT *
        FROM S3Object[*]['features'][*] as obj WHERE obj.properties.resolution = 8
    """
    logging.info(
        f"Extract H3 resolution 8 data from s3 bucket : {BUCKET_NAME} and filename : {H3_POLYGONS_LVL_8_9_10}")
    filtred_records = gpd.read_file(get_city_geojson(
        bucket_name=BUCKET_NAME, object_key=H3_POLYGONS_LVL_8_9_10, query_expression=city_polygons_extract, s3_client=s3_client), lines=True)

    # Fetch H3 resolution 8 data from S3 for data validation
    city_polygons_validation_extract = f"""
        SELECT *
        FROM S3Object[*]['features'][*] as obj
    """
    validation_records = gpd.read_file(get_city_geojson(
        bucket_name=BUCKET_NAME, object_key=H3_POLYGONS_LVL_8, query_expression=city_polygons_validation_extract, s3_client=s3_client), lines=True)

    # Validate H3 Resolution data
    validate_data_extract(filtered_extract=filtred_records,
                          validation_extract=validation_records)

    # Fetch Service Request Data & Join to City Polygons data
    sr_data = get_sr_data(bucket_name=BUCKET_NAME,
                          object_key=SERVICE_REQUEST_DATA, s3_client=s3_client)
    joined_sr_data = join_sr_to_gpd_data_extract(
        sr_data=sr_data, gpd_extract=validation_records)

    # Create a subsample of service requests that are within 1 minute of Bellville South
    location_centroid = get_location_centroid('BELLVILLE SOUTH')
    filtred_sr_data = filter_sr_data_by_distance(
        joined_sr_data, location_cetroid=location_centroid)

    # Add Wind data to subsample
    wind_df_raw = get_wind_data(WIND_DATA)
    wind_df_clean = clean_wind_data(wind_df_raw)
    sr_with_wind_data = merge_wind_data(sr_df=filtred_sr_data,
                                        wind_df=wind_df_clean, suburb='bellville')


if __name__ == "__main__":

    main()
