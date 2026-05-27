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

We address this problem by modeling energy production using past energy production and meteorological data as inputs predicting hour-by-hour the energy production for tomorrow without relying on same-day observations, which are not available at the time the bids are made.

## Stakeholders

Our primary stakeholders are established organizations that produce energy to sell on the market.

## Dataset

Our primary dataset for power production is the [Open Energy Data Initiative (OEDI) Photovoltaic Data Acquisition (PVDAQ) Public Datasets](https://data.openei.org/submissions/4568).  The relevant sub-data-sets are the [2023 Solar Prize Data](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2F2023-solar-data-prize%2F), containing rich data from 5 systems (4 usable), and the [PVDAQ General Collection](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fparquet%2F), a collection of 155 photovoltaic systems from the United States and India (82 systems with enough data; 11 systems with enough data and correct timezone information; 3 stress-free systems focused on for this initial project).  

Our primary dataset for daylight-and-sunlight hours, cloud cover, and irradiance is the [Open-Meteo historical weather-satellite data](https://open-meteo.com/en/docs/historical-weather-api), with hourly increment.  This allowed us to get Global Tilted Irradiance (Irradiance as adjusted for solar-panel tilt and solar angle) cheaply.

We still made a little use of another irradiance dataset, the NSRDB GOES-aggregated-v4 data, 1998-2024, with hourly or half-hourly increment.  See [NSRDB Satellites, General Info](https://nsrdb.nlr.gov/about/what-is-the-nsrdb) for general information, or [GOES-Aggregated-v4](https://developer.nlr.gov/docs/solar/nsrdb/nsrdb-GOES-aggregated-v4-0-0-download/) for more granular information.  For ease of access, we can use the [pvlib Python Package](https://pvlib-python.readthedocs.io/en/stable/index.html) accessors to access the data in a convenient way.  


### Complication -- size of documents

The raw data is too large to fit in the GitHub.  For example, we downloaded almost 10 GB of data from the [2023 Solar Data Prize dataset](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2F2023-solar-data-prize%2F) and 17.1 GB from the [PVDAQ Public Data Lake - Parquet](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fparquet%2F) collection (and even after trimming, we still need over 2 GB of data).  Hence, although most of the metadata is able to be stored in the GitHub folder, the bulk of the data is stored in the `data_ds_project` folder, a local-storage folder at the same level as the GitHub folder.

### Complication -- lots of preprocessing

There is a great deal of preprocessing needed to get the data into a usable state, not least of which are standardizing the units and variable names, catching changes in variable scale, integrating power measurements into hourly energy production, and restricting to high-quality-data days.  For preprocessing instructions, see [Build Order Instructions](src\data\build_order.md) or [Makefile](Makefile) and [makefile_notes](makefile_notes.md).  You may also simply take the end-result data from the courtesy copy located in [data\parquet_cleaned_energy](data/parquet_cleaned_energy/).  After that, use the [PreRun class](notebooks/modeling/RS/PreRun.py) for more localized before-modeling steps, including the integration of the OpenMeteo data.

## Modeling approach

### Functional Form

Our modeling assumption is that solar energy created (estimated) is a function of site data and previous energy values. Site data includes Sunlight Duration, Cloud cover, Irradiance (https://en.wikipedia.org/wiki/Irradiance), whether the panels are fixed or tracking, the tilt and azimuth of the panels if fixed (for angle-of-incidence reasons). (f. Previous Power incorporates 2-days-ago and annual lags  -- not one day prior, since we are simulating making decisions for tomorrow by 10 am *today*, when today's data is not in). Since we are only predicting a day ahead, and as historical forecast data is not generally available for our data sets, we use the actual meteorological data as a proxy, with the caveat that this would tend to make our model a little too accurate. We note that day-ahead forecasts are considered to be approximately 95% accurate.

### Target metric

Over-promising is expensive, as in such a case, you must purchase the shortfall in power from your competittors.  Hence, we used an asymmetric mean-squared-error that doubled the cost of over-estimates as compared to underestimates.  (For gradient-boosting models, we were even able to add this objective on.) 

### Models used

* Baseline (average prior Jan. 31st readings to predict this year's Jan. 31st reading)
* Linear Regression
* Time-series models (SARIMAX, Prophet)
* Gradient-Boosting Models (LightGBM, XGBoost)

### Predicting a Day Ahead and Cross-Validation Methods

If, in our story, we are attempting to predict the power on June 2, 2026, we are simulating our decision that must be sent by June 1, 2026, at 10 am.  Thus, we can only really use data from May 31st and earlier.  Hence, our basic cross-validation, given the possibility of bad days, and assuming a run of high-quality days for convenience, is something like the following picture.

![A picture of 5 folds in our cross-validation scheme.  The first fold is a run of 3 training days, followed by a skip day, then a validation day.  The second fold is a run of 4 training days, followed by a skip day, then a vaildation day.  Similarly for the other three folds.](kfold_pic.png)

Assuming all high-quality days, and padding by zero entries for hours with no data, this is comparable to a scikit-learn `TimeSeriesSplit(data, length=24, gap=24).`  In reality, there are a few more missing days in there.  Some caveats:

* For time-series methods, what we really do is predict 48 hours ahead of the training data, then drop the first 24 hours before applying our metric to record error.

* Indeed, our gradient-boosting methods were resource-intensive enough that I sometimes only used every `m`th fold, m between 2 and 5, to get effectively 250-fold to 350-fold cross-validation.

These folds are produced either by a the function `tts_of_data_using_end_days()` in the latest [PreRun](notebooks/modeling/RS/PreRun.py) or by the `k_fold_split_option_a(*args)` function in [notebooks/modeling/CEB/by_dates_Kfold.py](notebooks/modeling/CEB/by_dates_Kfold.py).

## Results

SARIMAX performed the best on the training set with regard to mean error and spread of errors, with the limitation that there had to be enough continuous data to allow it to find a clear prediction. The naive model, XGBoost, and LightGBM provided more competitive forecasts for later prediction dates since they had more data to train on.

## Future Directions

* Expand the model to work on other sites and other locations
* Continue to improve strategies to deal with downtime and missing data
* Explore approaches with the overestimation penalty as a hyperparameter
* Work with energy producers to standardize processes and better understand existing records

