census-pulse-schools
====================

* [What is this?](#what-is-this)
* [What scripts are included?](#what-scripts-are-included)
* [Where do I find the data?](#where-do-i-find-the-data)
* [How do I use it?](#how-do-i-use-it)
* [Other questions?](#other-questions)


What is this?
-------------

This repository contains scripts for downloading, slimming, and analyzing school-related data in the U.S. Census Bureau's "Household Pulse Survey." The data from this analysis will be used in a future St. Louis Post-Dispatch story.


What scripts are included?
--------------------------

1. `download.py` - A script for fetching the public-use files (PUFs) from census.gov. Requires you to specify the URLs of the desired PUF files in the text file `puf-urls.txt`.

2. `slim.py` - A script that reduces the size of the data by removing unneeded columns.

3. `analyze.py` - A script for calculating household totals and percentages from the survey data.


Where do I find the data?
-------------------------

The Census Bureau has a detailed website for the Pulse survey, including documentation:
https://www.census.gov/programs-surveys/household-pulse-survey.html


How do I use it?
----------------

The scripts in this repo require the use of Python 3.9, as well as the [agate](https://agate.readthedocs.io/en/latest/) data analysis library.

Use `pip install -r requirements3.txt` to install the necessary libraries into your environment (or virtual environment).

After that, just clone this repo and run the scripts in the order described above. They will produce a series of CSVs in the `data\analyzed` folder with the final percentages.


Other questions?
----------------

Contact St. Louis Post-Dispatch journalist [Josh Renaud](https://github.com/Kirkman/) at [jrenaud@post-dispatch.com](mailto:jrenaud@post-dispatch.com) with questions about this code.

Please contact the [U.S. Census Bureau](https://www.census.gov/programs-surveys/household-pulse-survey.html) to ask questions about the Household Pulse Survey or its data.

