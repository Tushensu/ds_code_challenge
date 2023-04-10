import logging
from src.utils.helper_functions import benchmark
import logging
import geopandas as gpd
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

    print("Here?")

    # Calculate the distance of 1 minute of latitude/longitude at the centroid's latitude
    lat_dist_per_min = distance(
        (location_cetroid.y, location_cetroid.x), (location_cetroid.y + 1, location_cetroid.x)).meters / 60
    lon_dist_per_min = distance(
        (location_cetroid.y, location_cetroid.x), (location_cetroid.y, location_cetroid.x + 1)).meters / 60

    buffer_dist = min(lat_dist_per_min, lon_dist_per_min)

    df_within_1_min = sr_data[sr_data.dist_to_centroid <= buffer_dist]

    return df_within_1_min
