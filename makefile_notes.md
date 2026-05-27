# Notes on Makefile

## Requirements
1. Access to UNIX-like `make` and `sed` commands.  Mac and *nix people should be settled, but Windows users will need to manually install them.  I (Charles) used GNUWin packages [make for Windows](https://gnuwin32.sourceforge.net/packages/make.htm) and [sed for Windows](https://gnuwin32.sourceforge.net/packages/sed.htm).  You may also need to add the GNUWin `/bin` directory to your system path -- see ``System Properties -- Advanced -- Environment Variables -- System Variables -- New``.
2. Python virtual environment or conda environment, 3.12 or 3.13-based.  It must be activated before running.  (I would need to have users manually include their activation files/links otherwise, and I'd need a `if/elif/else` structure for Windows command line, Windows PowerShell, and Unix commands.)
3. An NSRDB API key (https://developer.nlr.gov/signup/).  (Used to download a sample of irradiance data to verify that a day *should* be producing full power, to verify that the reported units of power are accurate.)
4. A folder `data_ds_project` on the same level as the GitHub folder, with `systems` subfolder.  (This might be superfluous.)

## Help
`make help` is instantiated, following the suggestions in [Stack Overflow Question 889035](https://stackoverflow.com/questions/8889035/how-to-document-a-makefile)  (this is where sed is used).  It can give further guidance. 

## Paths

### Part 0: Activate Environment and install files
As above, you must make and activate your Python virtual environment or conda environment on your own; that said, a `make install` command is given for convenience.

### Part 1: Download, extract, and clean the data
There is a lot of data, but a lot of it we use only for EDA, or not at all.  If you download all of it, you will be stuck for 6 hours or so; the short version should finish in about 75 minutes total and is sufficient to run all modeling.  Hence, we provide two paths.

**Option 1.** Use `make extract_and_call_all`, which will call the sequence of operations

```
download_power_all ; metadata_compiler_all ; download_weather_all ; extract_and_clean_all.
```
**Option 2.** Use `make extract_and_call_modeling_only`, which will activate the sequence
```
download_power_modeling_only ; metadata_compiler_modeling_only; download_weather_modeling_only ; make extract_and_clean_modeling_only
```

### Part 2: Do the modeling
Use the `make all_modeling_runs` command.

### Part 3: Run the final notebooks.
Run `make final_model_summaries`.