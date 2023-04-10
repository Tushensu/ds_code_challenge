import logging
from src.utils.helper_functions import benchmark
import logging
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from geopy.distance import distance


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
    geometry = [Point(xy)
                for xy in zip(sr_data.longitude, sr_data.latitude)]
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


@benchmark
def filter_sr_data_by_distance(sr_data, location_cetroid):
    """
    This function fil creates a subsample of the data by selecting all of the requests in sr data which are within 1 minute of the centroid of a given suburb

    Iput Parameters
    ---------------
    sr_data : Pandas.DataFrame
    location_cetroid : shapely.geometry.point.Point (tuple with latitude and longitude)


    Output
    ------
    filtred_sr_data : Pandas.DataFrame

    """

    # Drop rows without geometry data
    mask = sr_data['geometry'].apply(lambda geom: str(geom) == 'POINT EMPTY')
    sr_data = sr_data.loc[~mask]

    def dist_to_centroid(geometry):
        return distance((location_cetroid.y, location_cetroid.x), (geometry.y, geometry.x)).meters

    # Create a new column in the dataframe to store the distance from each point to the centroid
    sr_data['dist_to_centroid'] = sr_data.geometry.apply(dist_to_centroid)

    # Calculate the distance of 1 minute of latitude/longitude at the centroid's latitude
    lat_dist_per_min = distance(
        (location_cetroid.y, location_cetroid.x), (location_cetroid.y + 1, location_cetroid.x)).meters / 60
    lon_dist_per_min = distance(
        (location_cetroid.y, location_cetroid.x), (location_cetroid.y, location_cetroid.x + 1)).meters / 60

    buffer_dist = min(lat_dist_per_min, lon_dist_per_min)

    df_within_1_min = sr_data[sr_data.dist_to_centroid <= buffer_dist]

    return df_within_1_min


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


@benchmark
def clean_wind_data(df):
    """
    This function takes in an unprocessed pandas dataframe of wind data and return a cleaned version that is specific to a suburb


    Input Parameters
    ----------------
    df : Pandas.DataFrame


    Output
    ------
    df : Pandas.DataFrame
    """
    df.columns = [
        "  ".join(c).strip() if "Date" not in c[0] else c[0] for c in df.columns
    ]

    df = df.rename(columns={'Date & Time': 'timestamp_wind'})
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('__', '_')
    df.columns = df.columns.str.replace('m/s', 'Ms')
    df.columns = df.columns.str.lower()
    df = df.iloc[:-8]
    f = df.replace("NoData", None)
    df['timestamp_wind'] = pd.to_datetime(
        df['timestamp_wind'], format='%d/%m/%Y %H:%M', utc=True)

    if df.duplicated().sum() > 0:
        df = df.drop_duplicates()

    return df


@benchmark
def merge_wind_data(sr_df, wind_df, suburb):
    """
    This function auguments the filtered subsample of sr_data with the appropriate wind direction and speed data for 2020 for a specific suburb available in the wind data from 
    the Air Quality Measurement site.

    Input Parameters
    ----------------
    sr_df : Pandas.DataFrame
    wind_df : Pandas.DataFrame
    suburb : str


    Output
    ------
    merged_df : Pandas.DataFrame


    """

    filtred_wind_df = wind_df.filter(regex=f'^timestamp|^{suburb}')

    sr_df = sr_df.sort_values('creation_timestamp')
    sr_df['creation_timestamp'] = pd.to_datetime(
        sr_df['creation_timestamp'], format='%Y-%m-%d %H:%M:%S%z', utc=True)
    wind_df = filtred_wind_df.sort_values('timestamp_wind')

    merged_df = pd.merge_asof(
        sr_df, wind_df, left_on='creation_timestamp', right_on='timestamp_wind', direction='nearest')

    return merged_df
