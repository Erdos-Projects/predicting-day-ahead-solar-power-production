# A Guide to Build Order

The presumed base folder is `src/data/` in the GitHub folder, unless otherwise specified.

## Required packages

* `pandas`
* `boto3`
* `botocore`
* `jupyter` (for 1 or 2 `.ipynb` files)
* `json`
* `pyarrow`
* `tqdm` (nice progress bar)
* 


## Grab all the metadata and PVDAQ data.

1. Run `metadata_downloader.py`.  Saves metadata to the GitHub, in `../../data/raw/(subfolders)`.
2. Run `all_parquet_downloader.py`  Saves outside the GitHub Folder (space reasons -- approx. 17.5 GB of data), to `../../../data_ds_project/systems/parquet/`.
3. Run `prize_downloader.py` and run the cells in `prize_downloader_specific.ipynb`.  Saves outside the GitHub Folder to `../../../data_ds_project/systems/prize/` (about 10 GB on-disk)


## Clean up the systems metadata.

1. Run `systems_initializer.py`.  Makes `../../data/core/systems_cleaned.csv`.  Later runs modify this.
2. Run `systems_better_sample_year.py`
3. Run `systems_add_modules.py`.
4. Run `systems_sourced.py`.
5. Run `test_durations.py`.

(Teammates, this is probably already done for you, but just make sure the relevant columns are added whenever you open `systems_cleaned.csv`.)

## Grab the NSRDB sample-year data.

1. Grab an NSRDB API key from <https://developer.nlr.gov/signup/>.
2. Run `nsrdb_irradiance_sampler.py`, using the API key and the associated e-mail address.  (Teammates, also check your Google Drive.)

## Focus on cleaning up power data.

1.  Open `./pwr/ac_power_parquet_distiller_yearly.py`.  Make sure the full range of inputs is given in the start_index and end_index rules (lines 18-19), and the `parquet` save-type.
2.  Save and run the self-same file.  This will extract time and aggregate-power statistics from `../../../data_ds_project/systems/parquet/` for each *nice* parquet-saved system (at least 2 years' worth of AC power data), standardizing to kilowatts and removing some obvious outliers.  For each 'good' systems, we end up with a set of parquet files from a Pandas DataFrame (stratified by year) with `time` as the first column,  column, and `ac_power_kW` (appended by source type -- inverter, meter, or '' for unknown) as the middle columns, and `year` as the last column (need to add a year column to divide up the Parquet files by it).  Saves to `../../../data_ds_project/testing_yearly_parquet/` by default.

