#!/bin/sh

# zip required code files along with dependencies 
CURRENT_DIR=$(pwd)
cd libs
zip -r ../github-activity-lambda.zip .
cd ${CURRENT_DIR}
zip -g github-activity-lambda.zip lambda_function.py download.py upload.py utils.py