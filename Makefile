##------------------------------------------------------------
## Makefile for predicting-day-ahead-solar-power-production
## You are responsible for loading in your Python virtual environment
## or conda environment before running.
##----------------------------------------------------------
.POSIX:

# I made my files to be downloaded from a particular folder
# so I need a trick for file-separators (not for python, but for system cd)
# Idea from https://skramm.blogspot.com/2013/04/writing-portable-makefiles.html
ifdef ComSpec
	PATHSEP2:=\\
else
	PATHSEP2:=/
endif
PATHSEP=$(strip $(PATHSEP2))

ifndef ComSpec
    CMDSEP:=;
else
    CMDSEP:=&
endif

ifdef py
	PYRUNNER=py
else
	PYRUNNER=python3
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

# adapted from https://www.kdnuggets.com/the-case-for-makefiles-in-python-projects-and-how-to-get-started
.PHONY: install
install: ## No prereqs (except Python venv or conda env)
    ## Install environment.
	$(PYINSTALLER)

.PHONY: download_all
download_all: ## No prereqs (except Python venv or conda env)
    ## Install all data for all exploration and modeling.
    ## This will probably take 6+ hours, so be careful!
    ## Consider download_modeling_only if time is more limited.
    ## It also requires the NSRDB API key from https://developer.nlr.gov/signup/
	cd src$(PATHSEP)data && \
	$(PYRUNNER) metadata_downloader.py && \
	$(PYRUNNER) systems_initializer.py && \
	$(PYRUNNER) all_parquet_downloader.py && \
	jupyter nbconvert --to notebook --execute prize_downloader_specific.ipynb && \
	$(PYRUNNER) nsrdb_irradiance_sampler.py && \
	$(PYRUNNER) nsrdb_irradiance_full_downloader.py && \
	cd ..$(PATHSEP)..

.PHONY: metadata_compiler_all
metadata_compiler_all: download_all 
    ## add to the metadata dataframe
    ## test_durations.py might require some RAM.
	cd src$(PATHSEP)data && \
	$(PYRUNNER) systems_better_sample_year.py && \
	$(PYRUNNER) systems_add_modules.py && \
	$(PYRUNNER) systems_sourced.py && \
	$(PYRUNNER) test_durations.py && \
	cd ..$(PATHSEP)..

.PHONY: extract_and_clean_all
extract_and_clean_all: download_all metadata_compiler_all  
    ## extract and standardize Power Data, convert to energy data
    ## takes about 5 GB of RAM
	cd src$(PATHSEP)data$(PATHSEP)pwr && \
	$(PYRUNNER) ac_power_parquet_distiller_yearly.py && \
	cd ..$(PATHSEP)..$(PATHSEP).. && \
	cd notebooks$(PATHSEP)modeling$(PATHSEP)CEB && \
	$(PYRUNNER) better_clean_runner.py && \
	cd ..$(PATHSEP)..$(PATHSEP)..

.PHONY: download_modeling_only
download_modeling_only:  ## No prereqs beyond Python environment.
    ## Only downloads good-timezone systems.  Takes 30 min.
    ## It also requires the NSRDB API key from https://developer.nlr.gov/signup/
    ## Files meant to be run from containing folders, so lots of cd here.
	cd src$(PATHSEP)data && \
	$(PYRUNNER) metadata_downloader.py && \
	$(PYRUNNER) systems_initializer.py && \
	$(PYRUNNER) narrow_parquet_downloader.py && \
	$(PYRUNNER) narrow_nsrdb_irradiance_sampler.py && \
	cd ..$(PATHSEP)..

.PHONY: metadata_compiler_modeling_only
metadata_compiler_modeling_only: download_modeling_only  
    ## Download extended metadata only for good-timezone systems.
    ## narrow_test_durations will be a RAM hog
	cd src$(PATHSEP)data && \
	$(PYRUNNER) narrow_systems_better_sample_year.py && \
	$(PYRUNNER) systems_add_modules.py && \
	$(PYRUNNER) systems_sourced.py && \
	$(PYRUNNER) narrow_test_durations.py && \
	cd ..$(PATHSEP)..

.PHONY: extract_and_clean_modeling_only
extract_and_clean_modeling_only: download_modeling_only, metadata_compiler_modeling_only
    ## extract and standardize Power Data, convert to energy data
    ## still takes 5 GB of RAM
	cd src$(PATHSEP)data$(PATHSEP)pwr && \
	$(PYRUNNER) narrow_ac_power_parquet_distiller_yearly.py && \
	cd ..$(PATHSEP)..$(PATHSEP).. && \
	cd notebooks$(PATHSEP)modeling$(PATHSEP)CEB && \
	$(PYRUNNER) better_clean_runner.py && \
	cd ..$(PATHSEP)..$(PATHSEP)..

.PHONY: all_modeling_runs
all_modeling_runs: ## Requires one of extract_and_clean_modeling_only, extract_and_clean_all
    ## Run all modeling notebooks/scripts.
	cd notebooks$(PATHSEP)modeling$(PATHSEP)RS && \
	jupyter nbconvert --to notebook --execute naive_energy_forecaster.ipynb && \
	jupyter nbconvert --to notebook --execute linear_regression.ipynb && \
	jupyter nbconvert --to notebook --execute prophet.ipynb && \
	jupyter nbconvert --to notebook --execute sarimax.ipynb && \
	cd .. && \
	cd CEB && \
	jupyter nbconvert --to notebook --execute better_lightgbm_results.ipynb && \
	jupyter nbconvert --to notebook --execute xgboosting.ipynb && \
	cd ..$(PATHSEP)..$(PATHSEP)..

.PHONY: final_modeling_summaries
final_modeling_summaries: all_modeling_runs 
    ## Run summary files.  The pictures in final_results.ipynb may not work.
	cd notebooks$(PATHSEP)modeling && \
	jupyter nbconvert --to notebook --execute training_scores_comparison.ipynb && \
	jupyter nbconvert --to notebook --execute final_results.ipynb && \
	cd ..$(PATHSEP)..