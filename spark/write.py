
def to_files(df, tgt_dir, file_format):
    """
    writes dataframe to target directory
    @df dataframe to write
    @tgt_dir target directory
    @file_format one of csv, json or parquet
    """
    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'day'). \
        mode('append'). \
        format(file_format). \
        save(tgt_dir)