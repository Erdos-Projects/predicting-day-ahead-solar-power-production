# Predicting Day-Ahead Solar Energy Production

**Team Members:** Charles Baker, Will Grodzicki, Roberta Shapiro

**Special Thanks to:** Alex Myers, Praveen Shahani, Steven Gubkin

## Business context

With rising energy prices and concerns surrounding non-renewable energy sources, solar energy has become a major player in the energy industry. To participate in energy markets, power producers submit day-ahead bids specifying the amount of energy they expect to supply each hour the following day. Since the amount of solar energy that can be harvested is inconsistent, power producers must forecast the amount of energy they will be able to supply.  As power producers must buy any shortfall in power from their competitors on the spot market, overpromising is expensive, so good predictions should be somewhat conservative.

**Goal:** Create day-ahead hourly energy production predictions for use by power producers.

## Data Sources and Feature Engineering
We used public datasets for both solar-cell power production (3 Colorado sites from the  [PVDAQ Data Lake](https://data.openei.org/submissions/4568)) and weather satellites ([Open-Meteo Historical Weather](https://open-meteo.com/en/docs/historical-weather-api); h/t Alex Myers).  We aggregated power into hourly energy estimates and added appropriate lags.  We also used Fourier terms to express temporal features (time of day and year) as continuous features.

## Modeling

We compared 4 types of models: a baseline model that predicts production from the corresponding date in earlier years; linear regression; time-series models (SARIMAX, Prophet); and gradient-boosting models (XGBoost, LightGBM). In all cases, we measured performance with an asymmetric mean-squared error that doubled the penalty for overestimates compared to underestimates. (In practice, the factor should reflect the cost-profit tradeoff for the producer.)

## Results
SARIMAX performed the best on the training set with regard to mean error and spread of errors, with the limitation that there had to be enough continuous data to allow it to find a clear prediction. The naive model, XGBoost, and LightGBM provided more competitive forecasts for later prediction dates since they had more data to train on.

## Future Directions
* Expand the model to work on other sites and other locations
* Continue to improve strategies to deal with downtime and missing data
* Explore approaches with the overestimation penalty as a hyperparameter
* Work with energy producers to standardize processes and better understand existing records
