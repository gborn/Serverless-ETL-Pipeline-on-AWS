from pyspark.sql.functions import to_date
from utils import get_spark_session
import getpass

username = getpass.getuser()

env = os.environ.get('ENVIRON')
spark = get_spark_session(env, 'GitHub Activity - Reading Data')

 
src_file_path = f'/user/{username}/github-activity/landing/ghactivity'
src_df = spark.read.json(src_file_path)
src_df.printSchema()
src_df.show()
src_df.count()

src_df.groupBy(to_date('created_at').alias('created_at')).count().show()
 
tgt_file_path = f'/user/{username}/github-activity/raw/ghactivity'
tgt_df = spark.read.parquet(tgt_file_path)
tgt_df.printSchema()
tgt_df.show()
tgt_df.count()
tgt_df.groupBy('year', 'month', 'day').count().show()