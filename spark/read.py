
def from_files(spark, data_dir, file_pattern, file_format):
    """
    Reads files in given directory and returns spark dataframe
    @spark spark session object
    @data_dir directory to read files from
    @file_pattern prefix for files
    @file_format one of csv, json, or parquet
    @returns spark dataframe
    """
    df = spark. \
        read. \
        format(file_format). \
        load(f'{data_dir}/{file_pattern}')
    return df