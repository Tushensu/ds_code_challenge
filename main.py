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
from src.utils.constants import BUCKET_NAME, H3_POLYGONS_LVL_8_9_10, H3_POLYGONS_LVL_8, SERVICE_REQUEST_DATA
from src.utils.extract_data import get_city_geojson, get_sr_data
from src.utils.validation import validate_data_extract
from src.utils.helper_functions import get_location_centroid
from src.utils.transformations import join_sr_to_gpd_data_extract, filter_sr_data_by_distance, filter_sr_data_by_distance_optimized


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

    validate_data_extract(filtered_extract=filtred_records,
                          validation_extract=validation_records)

    sr_data = get_sr_data(bucket_name=BUCKET_NAME,
                          object_key=SERVICE_REQUEST_DATA, s3_client=s3_client)

    joined_sr_data = join_sr_to_gpd_data_extract(
        sr_data=sr_data, gpd_extract=validation_records)

    location_centroid = get_location_centroid('BELLVILLE SOUTH')
    filtred_sr_data = filter_sr_data_by_distance(
        joined_sr_data, location_cetroid=location_centroid)
    print(filtred_sr_data.head())

    filtred_sr_data = filter_sr_data_by_distance_optimized(
        joined_sr_data, location_cetroid=location_centroid)


if __name__ == "__main__":

    main()
