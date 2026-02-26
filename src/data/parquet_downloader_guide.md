# How to use `all_parquet_downloader.py`

## Requirements

1.  A Python version of at least 3.10 (for the `pathlib` and `boto3` libraries).
2.  A conda environment or Python virtual environment with the packages `pandas`, `boto3`, and `botocore`. See [Managing Conda Environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) or [venv](https://docs.python.org/3/library/venv.html) and [pip](https://packaging.python.org/en/latest/tutorials/installing-packages/) for more. 
3. A folder named `data_ds_project`, at the same level as the Github folder on your system.  Further, the folder should have a `systems` subfolder, and that folder should have a `parquet` subfolder.
4. The correct starting folder.  You must run the script from the src\data folder of the GitHub.

## Recommendations (Not Required)
1.  Run the commands from a standalone terminal, not the terminal in VSCode.  The only issue is that VSCode takes up a lot of RAM while running.  While the process itself is not RAM-intensive, whatever else you're working on while downloading files might be.

## Preferred Strategy
1. Go to Anaconda Prompt (or whatever terminal from which you can run python files).
2. Activate your conda environment or Python virtual environment.
3. Use the `cd` (change directory) command to move to the GitHub folder.
4. Again change the directory to the `src\data` subfolder of the GitHub folder.

### Option 1: download all at once.
5. Run the Python file.  On Windows, this would look like
``` 
py all_parquet_downloader.py
```
whereas on other systems this would look like
```
python3 all_parquet_downloader.py  
```

> Reminder: `ctrl-c` is the emergency-stop button if you need to stop for whatever reason.  When restarting, either mimic the ideas below to adjust starting indices, or just run as-is -- it will take about 5 seconds per `system-id` to review files it has already downloaded and realize that there is nothing to do.
### Option 2: download a bit at a time
5. From the folder in step 4, run `code all_parquet_downloader.py` (or your preferred Python code editor) to open up the Python file in VSCode.  Make adjustments in lines 10-19 as needed (e.g., change the ending index to 9 to download the first 10 systems' data), save and quit.
6.  Run the Python file.  On Windows, this would look like
``` 
py all_parquet_downloader.py
```
whereas on other systems this would look like
```
python3 all_parquet_downloader.py  
```
7. Use `code all_parquet_downloader.py` to open the file, edit the starting and ending indices, save and quit.
8. Run the Python file again.
9. Repeat steps 7-8 until done.
10. Since the program will never repeat steps, you can do one last run with all the indices (0-155) to make sure you do not

## Next Steps
1.  You can now use `practice_parquet_reader.ipynb` to explore each of the files, so long as you adjust the `system_id` for your system and `access_system_dir` accordingly.
2.  You can also use `prize_downloader.py` to download the environmental and irradiance data from the prize sites' datasets in the same way.  For electrical data, it's a lot more case-by-case, so see `prize_downloader_specific.ipynb`.
