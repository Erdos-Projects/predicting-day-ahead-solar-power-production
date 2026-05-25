## First lines from https://www.kdnuggets.com/the-case-for-makefiles-in-python-projects-and-how-to-get-started

.PHONY: help
help: ## yield names of instructions
    @echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

## I made my files to be downloaded from a particular folder
## so I need a trick for file-separators (not for python, but for CD)
## Idea from https://skramm.blogspot.com/2013/04/writing-portable-makefiles.html
ifdef ComSpec
    PATHSEP2=\\
else
    PATHSEP2=/
endif
PATHSEP=$(strip $(PATHSEP2))

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

.PHONY: install
install: ## install environments
    $(PYINSTALLER)

.PHONY: download_all
download_all: ## Install all data for all exploration and modeling.  This could take hours, so be careful!
    ## It also requires the NSRDB API key from https://developer.nlr.gov/signup/
	cd src$(PATHSEP)data
    $(PYRUNNER) metadata_downloader.py
	$(PYRUNNER) systems_initializer.py
	$(PYRUNNER) all_parquet_downloader.py 
	jupyter nbconvert --to notebook --execute prize_downloader_specific.ipynb
	$(PYRUNNER) nsrdb_irradiance_sampler.py
	$(PYRUNNER) nsrdb_irradiance_full_downloader.py
	cd ..$(PATHSEP)..

.PHONY: metadata_compiler_all
metadata_compiler_all: download_all  ## add to the metadata dataframe
	cd src$(PATHSEP)data
	$(PYRUNNER) systems_better_sample_year.py
	$(PYRUNNER) systems_add_modules.py
	$(PYRUNNER) systems_sourced.py
	$(PYRUNNER) test_durations.py
	cd ..$(PATHSEP)..

.PHONY: extract_and_clean_all
extract_and_clean_all: metadata_compiler_all  ## extract and standardize Power Data, convert to energy data
    cd src$(PATHSEP)data$(PATHSEP)pwr 
	$(PYRUNNER) ac_power_parquet_distiller_yearly.py
	cd ..$(PATHSEP)..$(PATHSEP)..
	cd notebooks$(PATHSEP)modeling$(PATHSEP)CEB
	$(PYRUNNER) better_clean_runner.py
	cd ..$(PATHSEP)..$(PATHSEP)..

.PHONY: download_modeling_only
download_modeling_only:  ## only downloads good-timezone systems.
    ## It also requires the NSRDB API key from https://developer.nlr.gov/signup/
    ## files meant to be run from src/data, so:
	cd src$(PATHSEP)data
    $(PYRUNNER) metadata_downloader.py
	$(PYRUNNER) systems_initializer.py
	$(PYRUNNER) narrow_parquet_downloader.py 
	$(PYRUNNER) narrow_nsrdb_irradiance_sampler.py
	cd ..$(PATHSEP)..

.PHONY: metadata_modeling_only:
metadata_modeling_only: download_modeling_only  ## Download extended metadata only for good-timezone systems.
    cd src$(PATHSEP)data
	$(PYRUNNER) narrow_systems_better_sample_year.py
	$(PYRUNNER) systems_add_modules.py
	$(PYRUNNER) systems_sourced.py
	$(PYRUNNER) narrow_test_durations.py
	cd ..$(PATHSEP)..

.PHONY: extract_and_clean_modeling_only
extract_and_clean_modeling_only: metadata_modeling_only  ## extract and standardize Power Data, convert to energy data
    cd src$(PATHSEP)data$(PATHSEP)pwr 
	$(PYRUNNER) narrow_ac_power_parquet_distiller_yearly.py
	cd ..$(PATHSEP)..$(PATHSEP)..
	cd notebooks$(PATHSEP)modeling$(PATHSEP)CEB
	$(PYRUNNER) better_clean_runner.py
	cd ..$(PATHSEP)..$(PATHSEP)..

