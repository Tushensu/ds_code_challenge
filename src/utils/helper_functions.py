from time import perf_counter
import logging
from geopy.geocoders import Nominatim
from shapely.geometry import Point


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
