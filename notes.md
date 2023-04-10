# Notes 

## Challenge 2

- For challenge 2 I have chosen a join error threshold of 25%, I would have preffered an even lower threshold <= 10% however for the data we are currently dealing with a 25% is reasonable.
- I decided on 25% in large part due to how much the join error is with our current set which is at 22.55%, setting a join error threshold that allows us to process the current dataset but with the expectation that this should improve in the future through investigating issues at source i.e. why we have incomplete data and also allowing for a little bit of room for more errors.

## Challenge 5

### 5.1
- For this task I decided to use the GeoPy Nominatim service to fetch the cetroid coordinates, this makes it simple and fast to get the cetroid for a given location.
- This service is the geocoder for OpenStreetMap data and is free to use.
- The centroid location is then used to calculate the distances between itself and the geopoint for the SR data
- In order to compute this without issues I chose to filter out data points without geometry data.

### 5.2
- For this task one of the most important things was the transformation and cleaning of the wind data that is downloaded directly from source
- A number of steps were taken to flatten the headers, standardise the column names and format the join data type

### 5.3
- In order to anonymise location data I used a technique that randomly displaces the original location within the specified accuracy, this means that for each piont we generate a new lat/long by first generating a random distance within the required accuracy and bearing then using these to calculate the new latitude and longitude using the Haversine formula, if the new location is not within the specified accuracy, the function repeats the process until a valid location is found.
- For anonymizing the timestamp data I used temporal cloaking which is a technique used to anonymize time series data by adding random noise to the timestamps without significantly altering the underlying temporal structure of the data
- Lastly I added further anonymization by converting all reference_numbers into uuids, this is done in case a reference number is linked to a person's personal info (e.g. a phone number or name and surname), then lastly choosing only spefic columns as the final output, with all the location data included only the data that has been anonymized as well as the same for timestamp data.
- The final subset does not have precise location or precise timestamp and there is not way to link it to any specific resident.
