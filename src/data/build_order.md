# A Guide to Build Order

The presumed base folder is `src/data/` in the GitHub folder, unless otherwise specified.

## Required packages for Build Order

* `pandas`
* `boto3`
* `botocore`
* `jupyter` (for 1 or 2 `.ipynb` files)
* `json`
* `pyarrow`
* `tqdm` (nice progress bar)
* `pvlib` (for optional objective at end)


## Download metadata and PVDAQ data.

### Option A.  Download all of it, including the stuff we'll rarely use

1. Run `metadata_downloader.py`.  Saves metadata to the GitHub, in `../../data/raw/(subfolders)`.
2. Run `systems_initializer.py`.  Makes `../../data/core/systems_cleaned.csv`.  (The next section modifies this file)
3. Run `all_parquet_downloader.py`  Saves outside the GitHub Folder (space reasons -- approx. 17.5 GB of data), to `../../../data_ds_project/systems/parquet/`.  See [Parquet Downloader Guide](parquet_downloader_guide.md) if there is any issue.  
4. (Optional until re-incorporated!) Run `prize_downloader.py` and run the cells in `prize_downloader_specific.ipynb`.  Saves outside the GitHub Folder to `../../../data_ds_project/systems/prize/` (about 10 GB on-disk)

### Option B.  Download only the systems we can use for modeling in the short term.

1.  Run `metadata_downloader.py`
2.  Run `systems_initializer.py`.
3.  Run `narrow_parquet_downloader.py`.


## Clean up the systems metadata.

### Option A: For all systems.

1. Run `systems_better_sample_year.py`
2. Run `systems_add_modules.py`.
3. Run `systems_sourced.py`.
4. Run `test_durations.py`.

(Teammates, this is probably already done for you, but just make sure the relevant columns are added whenever you open `systems_cleaned.csv`.)

### Option B: For only required systems.

1. Run `narrow_systems_better_sample_year.py`.
2. Run `systems_add_modules.py`.
3. Run `systems_sourced.py`.
4. Run `narrow_test_durations.py`.

## Grab the NSRDB sample-year data.

### Option A: Grab all of it.
1. Grab an NSRDB API key from <https://developer.nlr.gov/signup/>.
2. Run `nsrdb_irradiance_sampler.py`, using the API key and the associated e-mail address.  (Teammates, also check your Google Drive.)

### Option B: Only for modeling-required systems.
1. Grab an NSRDB API key from <https://developer.nlr.gov/signup/>.
2. Run `narrow_nsrdb_irradiance_sampler.py`, using the API key and the associated e-mail address.  (Teammates, also check your Google Drive.)

## Focus on cleaning up power data.

### Option A: Cleaning all of the rich-data systems.
1.  Open `./pwr/ac_power_parquet_distiller_yearly.py`.  Make sure the full range of inputs is given in the start_index and end_index rules (lines 18-19), and the `parquet` save-type.
2.  Save and run the self-same file.  This will extract time and aggregate-power statistics from `../../../data_ds_project/systems/parquet/` for each *nice* parquet-saved system (at least 2 years' worth of AC power data), standardizing to kilowatts and removing some obvious outliers.  For each 'good' systems, we end up with a set of parquet files from a Pandas DataFrame (stratified by year) with `time` as the first column,  column, and `ac_power_kW` (appended by source type -- inverter, meter, or '' for unknown) as the middle columns, and `year` as the last column (need to add a year column to divide up the Parquet files by it).  Saves to `../../../data_ds_project/testing_yearly_parquet/` by default.

### Option B: Only relevant systems
1. Run `./pwr/narrow_ac_power_parquet_distiller_yearly.py`.

## Run the Clean class to get energy data.

This is the class that integrates the short-interval power data into hourly energy data, and restricts to good-data days.

Other `.ipynb` files may do this already, but if it hasn't come up yet before the first use of PreRun:

1.  Run `../../notebooks/modeling/CEB/better_clean_runner.py`.  This will make the cleaned-energy files and save to `../../../data_ds_project/parquet_cleaned_energy`, for use by the various modeling codes.

(This is the data that is copied to the Github `data/parquet_cleaned_energy/`, for reference.)

That's it!  You're ready to run [PreRun](../../notebooks/modeling/RS/PreRun.py) and the various modeling codes in `../../notebooks/modeling/`.

### Very Optional -- Grab full NSRDB data to compile some less-necessary pictures in the weather subfolder.

Prior ideas (using NSRDB satellites more, some unneeded diagrams) encouraged us to do the full NSRDB download, back when we had 80 sites.

1.  Run `nsrdb_irradiance_full_downloader.py`.  