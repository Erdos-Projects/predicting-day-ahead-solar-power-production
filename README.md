# predicting-day-ahead-solar-power-production


Project completed for Erdos institute Data Science Bootcamp (spring-2026)


### Team members


1. [Roberta Shapiro](https://github.com/ShapiroRH)
2. [Charles Baker](https://github.com/ch83baker)
3. [William Grodzicki](https://github.com/wpgrodzicki)


### Acknowledgment


We give thanks for the assistance of [Alex Myers](https://github.com/MyersAlex924), who as a founding team member helped us find the [Open-Meteo historical weather-satellite data](https://open-meteo.com/en/docs/historical-weather-api), among other contributions in the early stage of the project.


## Project overview


We hope to develop tools for predicting hourly solar energy production, for use in the day-ahead energy market.


## Motivation and problem statement


Solar power is a growing portion of global energy production and is expected to play a significant role for the foreseeable future. 
To participate in energy markets, power producers must submit day-ahead bids specifying the amount of energy they expect to supply each hour the following day. (See, for example, [PJM Day-Ahead Market Information (2015 edition)](m11v72-energy-ancillary-services-market-ops.pdf).) These bids are based on forecasts that combine historical power production and predicted future environmental conditions.


We address this problem by modeling energy production using past energy production and meteorological data as inputs predicting hour-by-hour the power production for tomorrow without relying on same-day observations, which are not available at the time the bids are made.


## Stakeholders


Our primary stakeholders are established organizations that produce energy to sell on the market.


## Dataset


Our primary dataset for power production is the [Open Energy Data Initiative (OEDI) Photovoltaic Data Acquisition (PVDAQ) Public Datasets](https://data.openei.org/submissions/4568).  The relevant sub-data-sets are the [2023 Solar Prize Data](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2F2023-solar-data-prize%2F), containing rich data from 5 systems (4 usable), and the [PVDAQ General Collection](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fparquet%2F), a collection of 155 photovoltaic systems from the United States and India (82 usable).


Our primary dataset for daylight and sunlight hours is the [Open-Meteo historical weather-satellite data](https://open-meteo.com/en/docs/historical-weather-api), with hourly increment.


Our primary dataset for irradiance is the NSRDB GOES-aggregated-v4 data, 1998-2024, with hourly or half-hourly increment.  See [NSRDB Satellites, General Info](https://nsrdb.nlr.gov/about/what-is-the-nsrdb) for general information, or [GOES-Aggregated-v4](https://developer.nlr.gov/docs/solar/nsrdb/nsrdb-GOES-aggregated-v4-0-0-download/) for more granular information.  For ease of access, we use the [pvlib Python Package](https://pvlib-python.readthedocs.io/en/stable/index.html) accessors to access the data in a convenient way.  


In a similar way, we use the NSRDB GOES TMY (typical meteorological year) to find the “typical” data for a year.  See [GOES TMY](https://developer.nlr.gov/docs/solar/nsrdb/nsrdb-GOES-tmy-v4-0-0-download/) for more detailed information. 




### Complication -- size of documents
The raw data is too large to fit in the GitHub.  For example, we need almost 10 GB of data from the [2023 Solar Data Prize dataset](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2F2023-solar-data-prize%2F) and 17.1 GB from the [PVDAQ Public Data Lake - Parquet](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fparquet%2F) collection.  Hence, although most of the metadata is able to be stored in the GitHub folder, the bulk of the data is stored in the `data_ds_project` folder, a local-storage folder at the same level as the GitHub folder.




## Modeling approach
Our modeling assumption is that Power (estimated) is a function of site data and previous Power values. Site data includes Sunlight Duration, Irradiance (https://en.wikipedia.org/wiki/Irradiance), whether the panels are fixed or tracking, the tilt and azimuth of the panels if fixed (for angle-of-incidence reasons). (f. Previous Power incorporates daily and annual lags). Since we are only predicting a day ahead, and as historical forecast data is not generally available for our data sets, we use the actual meteorological data as a proxy, with the caveat that this would tend to make our model a little too accurate. We note that day-ahead forecasts are considered to be approximately 95% accurate.