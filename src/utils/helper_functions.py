import logging
import uuid
from time import perf_counter
from geopy.geocoders import Nominatim
from shapely.geometry import Point
from math import radians, sin, cos, sqrt, atan2


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
def get_location_centroid(location):
    """
    This function gets the centroid for a given location.

    Input Parameters
    ----------------
    location : str (This is the location name)

    Output
    ------
    tuple : (latitude , longitude)
    """

    geolocator = Nominatim(user_agent="my_application")
    location_geocode = geolocator.geocode(location)
    location_centroid = Point(
        location_geocode.longitude, location_geocode.latitude)

    return location_centroid


def generate_uuid(val):
    """
    This function generates a random uuid for a given value.

    Input Parameters
    ----------------
    val


    Output
    ------
    uuid : str 
    """

    return uuid.uuid4().hex


def calculate_distance(lat1, lon1, lat2, lon2):
    """
    This function calculates the distance between two points given their latitude and longitude.

    Input Parameters
    ----------------
    lat1 : float
    lat2 : float
    lon1 : float
    lon2 : float

    Output
    -----
    distance : float

    """

    R = 6371
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c * 1000
