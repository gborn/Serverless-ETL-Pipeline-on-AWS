#!/bin/sh

# set environment variables
# make sure SRC_DIR and TGT_DIR are correct before running
export ENVIRON=PROD
export SRC_DIR=/user/${USER}/github-activity-gb/landing/ghactivity
export SRC_FILE_FORMAT=json
export TGT_DIR=/user/${USER}/github-activity-gb/raw/ghactivity
export TGT_FILE_FORMAT=parquet
 
export PYSPARK_PYTHON=python3

# running for day 1
export SRC_FILE_PATTERN=2022-04-01
 
spark2-submit --master yarn \
    --py-files ghactivity.zip \
    app.py
 
# running for day 2
export SRC_FILE_PATTERN=2022-04-02
 
spark2-submit --master yarn \
    --py-files ghactivity.zip \
    app.py
 
# running for day 3
export SRC_FILE_PATTERN=2022-04-03
 
spark2-submit --master yarn \
    --py-files ghactivity.zip \
    app.py
