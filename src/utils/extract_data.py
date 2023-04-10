import botocore.exceptions
import logging
import pandas as pd
from src.utils.helper_functions import benchmark


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
            logging.error(f'The object does not exist.')
        else:
            logging.error(f'An error occurred: {e}')
    except botocore.exceptions.ParamValidationError as e:
        logging.error(f'Invalid input parameters: {e}')
    except Exception as e:
        logging.error(f'An error occurred: {e}')

    return records


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
def get_wind_data(url):
    """
    This function downloads wind data from a url where the wind data is stored as an excel file

    Input Parameters
    ----------------
    url : str (the url that contains wind data in excel format)

    Output
    ------
    df : pandas.DataFrame object
    """

    df = pd.read_excel(url, skiprows=2,  header=[0, 1, 2])

    return df
