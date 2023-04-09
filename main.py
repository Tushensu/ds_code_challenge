import boto3
import botocore.exceptions
import logging
import configparser
import os
import pandas as pd
import geopandas as gpd
from datetime import datetime
from time import perf_counter
from utils.constants import BUCKET_NAME, H3_POLYGONS_LVL_8_9_10, H3_POLYGONS_LVL_8

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="logs/cpt_ds_challenge.log"
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
def get_city_geojson(bucket_name, object_key, query_expression):
    '''
    This function extracts H3 resolution 8 data from S3

    Input Parameters
    ----------------
    bucket_name : string (this is the name of the AWS S3 bucket)
    object_key : string (this is the json object file to read data from)
    resolution : int (this is the resolution to use when filtering data from the json file)

    Output
    ------
    list[dict] : A list of the json objects extracted

    '''

    # Set up S3 client
    s3 = boto3.client('s3', region_name='af-south-1')
    input_serialization = {'JSON': {'Type': 'DOCUMENT'}}
    output_serialization = {'JSON': {}}
    records = ''

    # Use S3 Select to get filtered data from JSON file
    try:

        response = s3.select_object_content(
            Bucket=bucket_name,
            Key=object_key,
            Expression=query_expression,
            ExpressionType='SQL',
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization
        )

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("The object does not exist.")
        else:
            print("An error occurred: ", e)
    except botocore.exceptions.ParamValidationError as e:
        print("Invalid input parameters:", e)
    except Exception as e:
        print("An error occurred: ", e)

    # Iterate over the response and print each row of filtered data
    for event in response['Payload']:
        if 'Records' in event:
            records += event['Records']['Payload'].decode('utf-8')

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


def main():
    """
    This is the main function that runs the entire program
    """

    # Fetch H3 resolution 8 data from S3
    city_polygons_extract = f"""
        SELECT * 
        FROM S3Object[*]['features'][*] as obj WHERE obj.properties.resolution = 8
    """
    logging.info(
        f"Extract H3 resolution 8 data from s3 bucket : {BUCKET_NAME} and filename : {H3_POLYGONS_LVL_8_9_10}")
    filtred_records = gpd.read_file(get_city_geojson(
        bucket_name=BUCKET_NAME, object_key=H3_POLYGONS_LVL_8_9_10, query_expression=city_polygons_extract), lines=True)

    # Fetch H3 resolution 8 data from S3 for data validation
    city_polygons_validation_extract = f"""
        SELECT * 
        FROM S3Object[*]['features'][*] as obj
    """
    validation_records = gpd.read_file(get_city_geojson(
        bucket_name=BUCKET_NAME, object_key=H3_POLYGONS_LVL_8, query_expression=city_polygons_validation_extract), lines=True)

    validate_data_extract(filtered_extract=filtred_records,
                          validation_extract=validation_records)


if __name__ == "__main__":

    main()
