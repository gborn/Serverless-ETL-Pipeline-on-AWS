
from pyspark.sql import SparkSession
 

def get_spark_session(env, app_name):
    """
    returns spark session
    """
    # local cluster
    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(app_name). \
            getOrCreate()
        return spark

    # production cluster with yarn as resource manager
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            appName(app_name). \
            getOrCreate()
        return spark

    return