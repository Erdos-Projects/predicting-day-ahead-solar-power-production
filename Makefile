##------------------------------------------------------------
## Makefile for predicting-day-ahead-solar-power-production
## You are responsible for loading in your Python virtual environment
## or conda environment before running.
##----------------------------------------------------------
.POSIX:

# I made my files to be downloaded from a particular folder
# so I need a trick for file-separators (not for python, but for system cd)
# Idea from https://skramm.blogspot.com/2013/04/writing-portable-makefiles.html
# Note: path separators appear to be fixed.
ifdef ComSpec
	PATHVAR2:=\\
else
	PATHVAR2:=/
endif
PATHVAR=$(strip $(PATHVAR2))

ifndef ComSpec
    CMDSEP:=;
else
    CMDSEP:=&
endif

# note: see practice_seps below for why I choose python over python3 for the python runner
# It appears to correctly run in the venv, assuming one is activated.
ifdef py
	PYRUNNER=py
else
	PYRUNNER=python
endif

ifdef conda
	PYINSTALLER=conda install --file requirements.txt
else
	PYINSTALLER=pip install -r requirements.txt
endif

# From https://stackoverflow.com/questions/8889035/how-to-document-a-makefile
.PHONY: help
help: ## No prereqs
    ## yield names of instructions, Windows environment
    ## Requires sed command as well.
	@echo "Available commands:"
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)
.DEFAULT_GOAL := help

.PHONY: practice_seps
practice_seps: ## No prereqs (except Python venv or conda env)
    ## Quick debugger for path separator
	cd src\data
	cd src\data && $(PYRUNNER) test_hello.py
	cd src/data
	cd src/data && $(PYRUNNER) test_hello.py
	cd src\\data 
	cd src\\data && $(PYRUNNER) test_hello.py

# adapted from https://www.kdnuggets.com/the-case-for-makefiles-in-python-projects-and-how-to-get-started
.PHONY: install
install: ## No prereqs (except Python venv or conda env)
    ## Install environment.
	$(PYINSTALLER)

.PHONY: download_power_all
download_power_all: ## No prereqs (except Python venv or conda env)
    ## Install all power data for all exploration and modeling.
    ## This will probably take 6+ hours, so be careful!
    ## Consider download_power_modeling_only if time is more limited.
	cd src/data && \
	$(PYRUNNER) metadata_downloader.py && \
	$(PYRUNNER) systems_initializer.py && \
	$(PYRUNNER) all_parquet_downloader.py && \
	jupyter nbconvert --to notebook --execute prize_downloader_specific.ipynb && \
	$(PYRUNNER) nsrdb_irradiance_sampler.py && \
	$(PYRUNNER) nsrdb_irradiance_full_downloader.py && \
	cd ../..

.PHONY: download_weather_all
download_weather_all: download_power_all ## 
    ## Download sample weather data (for later power verification)
    ## and full weather data (for one set of diagrams in weather folder)
    ## It requires the NSRDB API key from https://developer.nlr.gov/signup/
	cd src/data && \
	$(PYRUNNER) systems_better_sample_year.py && \
	$(PYRUNNER) nsrdb_irradiance_sampler.py && \
	$(PYRUNNER) nsrdb_irradiance_full_downloader.py && \
	cd ../..

.PHONY: metadata_compiler_all
metadata_compiler_all: download_power_all ## 
    ## add to the metadata dataframe
    ## test_durations.py might require some RAM.
	cd src/data && \
	$(PYRUNNER) systems_add_modules.py && \
	$(PYRUNNER) systems_sourced.py && \
	$(PYRUNNER) test_durations.py && \
	cd ../..

.PHONY: extract_and_clean_all
extract_and_clean_all: download_power_all metadata_compiler_all download_weather_all ## 
    ## extract and standardize Power Data, convert to energy data
	cd src/data/pwr && \
	$(PYRUNNER) ac_power_parquet_distiller_yearly.py && \
	cd ../../.. && \
	cd notebooks/modeling/CEB && \
	$(PYRUNNER) better_clean_runner.py && \
	cd ../../..

.PHONY: download_power_modeling_only
download_power_modeling_only:  ## No prereqs beyond Python environment.
    ## Only downloads good-timezone systems.  Takes 30 min.
    ## It also requires the NSRDB API key from https://developer.nlr.gov/signup/
    ## Files meant to be run from containing folders, so lots of cd here.
	cd src/data && \
	$(PYRUNNER) metadata_downloader.py && \
	$(PYRUNNER) systems_initializer.py && \
	$(PYRUNNER) narrow_parquet_downloader.py && \
	cd ../..

.PHONY: download_weather_modeling_only
download_weather_modeling_only: download_power_modeling_only ## 
    ## Download sample weather data (for later power verification)
    ## It requires the NSRDB API key from https://developer.nlr.gov/signup/
	cd src/data && \
	$(PYRUNNER) narrow_systems_better_sample_year.py && \
	$(PYRUNNER) narrow_nsrdb_irradiance_sampler.py && \
	cd ../..

.PHONY: metadata_compiler_modeling_only
metadata_compiler_modeling_only: download_power_modeling_only ## 
    ## Download extended metadata only for good-timezone systems.
    ## narrow_test_durations will be a RAM hog
	cd src/data && \
	$(PYRUNNER) systems_add_modules.py && \
	$(PYRUNNER) systems_sourced.py && \
	$(PYRUNNER) narrow_test_durations.py && \
	cd ../..

.PHONY: extract_and_clean_modeling_only
extract_and_clean_modeling_only: download_power_modeling_only metadata_compiler_modeling_only download_weather_modeling_only ## 
    ## extract and standardize Power Data, convert to energy data
	cd src/data/pwr && \
	$(PYRUNNER) narrow_ac_power_parquet_distiller_yearly.py && \
	cd ../../.. && \
	cd notebooks/modeling/CEB && \
	$(PYRUNNER) better_clean_runner.py && \
	cd ../../..

.PHONY: all_modeling_runs
all_modeling_runs: ## Requires one of extract_and_clean_modeling_only, extract_and_clean_all
    ## Run all modeling notebooks/scripts.
	cd notebooks/modeling/RS && \
	$(PYRUNNER) naive_energy_forecaster_static.py && \
	$(PYRUNNER) linear_regression_static.py && \
	$(PYRUNNER) sarimax_static.py && \
	$(PYRUNNER) prophet_static.py && \
	cd .. && \
	cd CEB && \
	$(PYRUNNER) lightgbm_static.py && \
	$(PYRUNNER) xgboost_static.py && \
	cd ../../..

.PHONY: final_modeling_summaries
final_modeling_summaries: all_modeling_runs ## 
    ## Run summary files.  The pictures in final_results.ipynb may not work.
	cd notebooks/modeling && \
	$(PYRUNNER) training_scores_comparison_static.py && \
	$(PYRUNNER) final_results_static.py && \
	cd ../..