import logging
from src.utils.helper_functions import benchmark


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
