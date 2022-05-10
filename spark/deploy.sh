
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf "spark.yarn.appMasterEnv.ENVIRON=PROD" \
    --conf "spark.yarn.appMasterEnv.SRC_DIR=/user/hadoop/prod/landing/ghactivity" \
    --conf "spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json" \
    --conf "spark.yarn.appMasterEnv.TGT_DIR=/user/hadoop/prod/raw/ghactivity/" \
    --conf "spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet" \
    --conf "spark.yarn.appMasterEnv.SRC_FILE_PREFIX=2021-01-15 \
    --py-files ghactivity.zip\
    app.py