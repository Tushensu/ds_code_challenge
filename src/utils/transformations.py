import logging
import logging
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
from geopy.distance import distance
from src.utils.helper_functions import calculate_distance, generate_uuid
from src.utils.helper_functions import benchmark


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


@benchmark
def anonymize_sr_data(df, lat_col, lon_col, location_accuracy=500, temporal_accuracy=6):
    """
    This function anonymises the filtered subsample of sr_data preserves the following precisions :
    - location accuracy to within approximately 500m
    - temporal accuracy to within 6 hours

    Input Parameters
    ----------------
    df : Pandas.DataFrame
    lat_col : str
    lon_col : str
    location_accuracy : int
    temporal_accuracy : int

    Output
    ------
    df : Pandas.Dataframe (the anonymized result set)


    """

    n = len(df)
    lat = df[lat_col].values
    lon = df[lon_col].values
    new_lat = np.zeros(n)
    new_lon = np.zeros(n)
    for i in range(n):
        # Generate a random displacement within the specified accuracy
        displacement = np.random.uniform(0, location_accuracy)
        bearing = np.random.uniform(0, 360)
        lat1, lon1 = lat[i], lon[i]
        lat2 = lat1 + (displacement / 1000) * np.sin(np.deg2rad(bearing))
        lon2 = lon1 + (displacement / 1000) * \
            np.cos(np.deg2rad(bearing)) / np.cos(np.deg2rad(lat1))

        # Check if the new location is within the specified accuracy
        while calculate_distance(lat1, lon1, lat2, lon2) > location_accuracy:
            displacement = np.random.uniform(0, location_accuracy)
            bearing = np.random.uniform(0, 360)
            lat2 = lat1 + (displacement / 1000) * np.sin(np.deg2rad(bearing))
            lon2 = lon1 + (displacement / 1000) * \
                np.cos(np.deg2rad(bearing)) / np.cos(np.deg2rad(lat1))
        new_lat[i] = lat2
        new_lon[i] = lon2
    df[lat_col] = new_lat
    df[lon_col] = new_lon

    period = pd.Timedelta(hours=temporal_accuracy)
    df['creation_timestamp'] = pd.to_datetime(
        df['creation_timestamp'], format='%Y-%m-%d %H:%M:%S%z', utc=True)
    df['creation_timestamp'] = df['creation_timestamp'].dt.floor(period)

    df['completion_timestamp'] = pd.to_datetime(
        df['completion_timestamp'], format='%Y-%m-%d %H:%M:%S%z', utc=True)
    df['completion_timestamp'] = df['completion_timestamp'].dt.floor(period)

    df['timestamp_wind'] = df['timestamp_wind'].dt.floor(period)
    df['reference_number'] = df['reference_number'].apply(generate_uuid)

    return df
