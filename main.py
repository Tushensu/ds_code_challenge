import boto3
import botocore.exceptions
import logging
import configparser
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.geocoders import Nominatim
from pathlib import Path
from datetime import datetime
from time import perf_counter
from utils.constants import BUCKET_NAME, H3_POLYGONS_LVL_8_9_10, H3_POLYGONS_LVL_8, SERVICE_REQUEST_DATA


# Set paths
HOME_DIR = Path.cwd()
DATA_DIR = HOME_DIR / "data"
LOGS_DIR = HOME_DIR / "logs"

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename=LOGS_DIR / "cpt_ds_challenge.log"
                    )


# Get AWS keys
config = configparser.ConfigParser()
config.read_file(open('utils/dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get(
    'AWS', 'AWS_SECRET_ACCESS_KEY')


def benchmark(func: callable(...)):
    """
    This function is used to calculate benchmarks for other functions

    Input Parameters
    ----------------
    func : any valid python function


    Output
    ------
    wrapper : python function
    """

    def wrapper(*args, **kwargs):
        """
        This is the wrapper function that computes the runtime for a given function

        """
        start_time = perf_counter()
        value = func(*args, **kwargs)
        end_time = perf_counter()
        run_time = end_time - start_time
        logging.info(
            f"Execution of {func.__name__} took {run_time:.2f} seconds.")
        return value

    return wrapper


@benchmark
def get_city_geojson(bucket_name, object_key, query_expression, s3_client):
    '''
    This function extracts H3 resolution 8 data from S3

    Input Parameters
    ----------------
    bucket_name : string (this is the name of the AWS S3 bucket)
    object_key : string (this is the json object file to read data from)
    s3_client : botocore.client.S3 (The S3 client object)

    Output
    ------
    list[dict] : A list of the json objects extracted

    '''

    input_serialization = {'JSON': {'Type': 'DOCUMENT'}}
    output_serialization = {'JSON': {}}
    records = ''

    # Use S3 Select to get filtered data from JSON file
    try:

        response = s3_client.select_object_content(
            Bucket=bucket_name,
            Key=object_key,
            Expression=query_expression,
            ExpressionType='SQL',
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization
        )

        # Iterate over the response and print each row of filtered data
        for event in response['Payload']:
            if 'Records' in event:
                records += event['Records']['Payload'].decode('utf-8')

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("The object does not exist.")
        else:
            print("An error occurred: ", e)
    except botocore.exceptions.ParamValidationError as e:
        print("Invalid input parameters:", e)
    except Exception as e:
        print("An error occurred: ", e)

    return records


@benchmark
def validate_data_extract(filtered_extract, validation_extract) -> bool:
    """
    This functions validates whether or not 2 record sets are equal to eachother

    Input Parameters
    ----------------
    filtered_extract : geopandas.DataFrame
    validation_extract : geopandas.DataFrame

    Output
    ------
    validated : bool

    """

    # Drop resolution column from the filtered extract & validate results
    filtered_extract = filtered_extract.drop(
        columns=['resolution'])

    validated = False
    if filtered_extract.equals(validation_extract):

        validated = True
        logging.info(
            f"Data validation  for city polygons data PASSED. Extracted data == Validation data :  validation test : {validated}")
    else:
        logging.info(
            f"Data validation  for city polygons data FAILED. Extracted data == Validation data :  validation test : {validated}")

    return validated


@benchmark
def get_sr_data(bucket_name, object_key, s3_client):
    """
    This function extacts the service request data from S3

    Input Parameters
    ----------------
    bucket_name : str (This is the S3 bucket name)
    object_key : str (This is the csv object name in the S3 bucket to read from)
    s3_client : botocore.client.S3 (The S3 client object)


    Output
    ------
    df : pandas.DataFrame object 
    """

    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    df = pd.read_csv(obj['Body'], compression='gzip',
                     index_col=0).reset_index(drop=True)

    return df


@benchmark
def join_sr_to_gpd_data_extract(sr_data, gpd_extract):
    """
    This function joins service request data to H3 resolution  level 8 data extract dataframes based on geometry and points
    It also assigns each service request to a single H3 resolution level 8 hexagon.

    Input Parameters
    ----------------
    sr_data : Pandas.Daframe
    gpd_extract : GeoPandas.DataFrame


    Output
    ------
    joined_sr_request_df : GeoPandas.DataFrame

    """

    # Create geometry points for each SR and convert sr_data to a GeoDataFrame and adding the newly created goemetry point column
    # and using the same CRS as the H3 data extract
    geometry = [Point(xy) for xy in zip(sr_data.longitude, sr_data.latitude)]
    sr_gpdf = gpd.GeoDataFrame(sr_data, crs=gpd_extract.crs, geometry=geometry)

    # Join the two dataframes and caulate join error percentage
    joined_sr_request_df = gpd.sjoin(
        sr_gpdf, gpd_extract, how='left', predicate='within')
    failed_joins_sum = joined_sr_request_df['index'].isna().sum()
    failed_joins_perc = (failed_joins_sum / len(joined_sr_request_df)) * 100
    joined_sr_request_df['index'].fillna('0', inplace=True)

    logging.info(
        f'Failed to join {failed_joins_sum} of the sr data to H3 resolution level 8 hexagon data. This is a join error of {failed_joins_perc:.2f}%')
    if failed_joins_perc > 25:
        raise Exception(
            f'Failed to join {failed_joins_perc:.2f} % of the records')  # Raise Error if join percentage is greater than 25 %

    return joined_sr_request_df


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

    df = get_sr_data(bucket_name=BUCKET_NAME,
                     object_key=SERVICE_REQUEST_DATA, s3_client=s3_client)

    print(df.head())

    join_sr_to_gpd_data_extract(df, validation_records)


if __name__ == "__main__":

    main()
