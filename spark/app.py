import os
from utils import get_spark_session
from read import from_files
from transform import transform
from write import to_files
 
 
def main():
    """
    main driver program
    """
    # set environment variables
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')

    # create spark session
    spark = get_spark_session(env, 'GitHub Activity - Reading Data')

    # read files
    df = from_files(spark, src_dir, file_pattern, src_file_format)

    # preprocess and transform
    df_transformed = transform(df)

    # store transformed dataframe
    to_files(df_transformed, tgt_dir, tgt_file_format)

    df_transformed.printSchema()
    df_transformed.select('repo.*').show()
 
 
if __name__ == '__main__':
    main()