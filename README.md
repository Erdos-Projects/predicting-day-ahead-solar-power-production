# spring-2026-solar-panel-degradation-team-2
Project completed for Erdos institute data science bootcamp (fall-2025)

### Team members
1. [Roberta Shapiro](https://github.com/ShapiroRH)
2. [Charles Baker](https://github.com/ch83baker)
3. [William Grodzicki](https://github.com/wpgrodzicki)

## Project overview
We hope to develop tools for analyzing and predicting photovoltaic-cell degradation in solar panels using location, meteorological data, and installation type

## Motivation and problem statement
Solar power is a growing portion of energy production in the United States and will likely continue to play a significant role for the foreseeable future. As such, it is important to understand and be able to predict the long term state of solar farms, in order to plan for future growth, replacement, and maintenance.

In this project we seek to answer the following question: “Can we use meteorological data and patterns to predict future degradation of solar cells?”

## Stakeholders
Our primary stakeholders are companies and governments that are planning to construct and/or maintain solar farms, as this project aims to predict their long term health and performance.

## Dataset

Our initial EDA was from the 2022 "Photovoltaic Fleet Degradation Insights" dataset. [Data](https://datahub.duramat.org/dataset/photovoltaic-fleet-degradation-insights-data) [Metadata](https://datahub.duramat.org/dataset/metadata/photovoltaic-fleet-degradation-insights-data), which, although it only records very vague location data, gives a good sense of what can be done with [RdTools](https://www.nlr.gov/pv/rdtools), one of our principal packages.

As most Duramat data appears to be limited to members of authorized teams, our primary dataset is the [Open Energy Data Initiative (OEDI) Photovoltaic Data Acquisition (PVDAQ) Public Datasets](https://data.openei.org/submissions/4568).  See the [Data Collection Strategy Document](data_collection_strategy.md) for more of a sense of the data.  

As an aside, we note that most installations in the sub-group [PVDAQ Public Data Lake - CSV](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fcsv%2F) collect AC power/current/voltage data and nothing else.  This is not enough to use RdTools.  

Our predictive features include latitude, longitude, ZIP, meteorological data, and type of installation

### Complication -- size of documents
Although the CSV Data Lake is not relvant for this study, the [2023 Solar Data Prize dataset](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2F2023-solar-data-prize%2F) is 450.54 GB of csv files, although we only need some of the data and we can parquet-save it to save on space.

The [PVDAQ Public Data Lake - Parquet](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=pvdaq%2Fparquet%2F) collection, however, is around 17.5 GB of data.  Even restricting to the data we think we need, it is over 10 GB.  


## Modeling approach
Our modeling assumption is more-or-less Degradation (estimated) = f(Site), where site data includes location (latitude, longitude), meteorological data (both locally recorded and the [NSRDB satellite data](https://nsrdb.nrel.gov/)), the type of materials in the solar cell, and the database data on Power generation, Ambient or Module Temperature, and something called [irradiance](https://en.wikipedia.org/wiki/Irradiance).  

### Getting the degradation estimates via RdTools
Actually getting estimates of our response variable, degradation, is a nontrivial amount of work.  The steps are as follows, as adapted from https://rdtools.readthedocs.io/en/stable/examples/degradation_and_soiling_example.html.

1.  Load in the metadata for each site.
2.  Either load in the existing module temperature, or use [pvlib](https://rdtools.readthedocs.io/en/stable/examples/degradation_and_soiling_example.html) with the ambient-temperature records to simulate the module temperature.
3.  Load in the power-generation and irradiance data.
4.  Use pvlib again to get a normalized power estimate.
5.  Filter out some noise in the data.
6.  Aggregate the irradiance data per day with a weighted average.  (Because start-of-day and end-of-day measurements are very noisy.)
7.  More filtering.
8.  Get degradation estimate.
9.  Correct for any soiling.  If the panel has accumulated dust on top, and it gets cleaned off all at once, we need to correct for the spike in power.

### Stretch goal -- PVDeg as a stronger competitor than DummyRegressor.
The [PVDeg](https://pvdegradationtools.readthedocs.io/en/latest/#) package is also relevant to our analysis.  It is purely simulated data, but it has some strong numerical-methods behind it.  (I seem to recall the phrase, "finite-element analysis", being tossed around.)

Basically, the workflow there is:
1.  Input the location and the type of solar panel.
2.  Grab the NSRDB weather data.
3.  Choose which diferential equation model (out of the pre-built models) to use to model degradation.
4.  Compute the Monte Carlo simulations -- for one location on a local computer, for many locations via a Google Colab system to access the stronger computer power.

See (89537.pdf) for an accessible overview.

We are not quite certain that we can use it, but if we can, it would be interesting to compare our careful statistical work to these quick estimates, to see if we do better or worse with all of that historical data. 

