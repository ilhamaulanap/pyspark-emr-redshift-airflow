import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl,
                               IntegerType as Int, TimestampType as Timestamp, 
                               DateType as Date)
from pyspark.sql.functions import (col, year, month, dayofmonth, weekofyear, quarter)


redshift_url = "jdbc:redshift://aws-redshift-cluster-1:85023052235:5439/movies_analytics" #change using your own redshift jbdc url
redshift_properties = {
    "user": "#Username", #change using your own redshift username 
    "password": "#password", # change using your own redshift password
    "driver": "com.amazon.redshift.jdbc41.Driver"
}

def create_spark_session():
    """
    SparkSession a unified conduit to all Spark operations and data, and it is an entry point to start programming with DataFrame and Dataset.  
    .getOrCreate() option checks if there is an existing SparkSession otherwise it creates a new one
    """
    spark = SparkSession.builder.\
        config("spark.jars.packages","com.amazon.redshift:redshift-jdbc42:2.1.0.21")\
        .getOrCreate()
    
    return spark


## read csv data
def process_data(spark, file_input, output):


    # delete Null values
    ratings_schema = StructType([
        Fld("userId", Int()),
        Fld("movieId", Int()),
        Fld("rating", Dbl()),
        Fld("timestamp", Timestamp())
    ])

    ratings_df = spark.read.option("header", "true") \
                           .csv(f"{file_input}/ratings.csv", 
                                schema=ratings_schema)

    ratings_df = ratings_df.select(
        col("userId").alias("user_id"),
        col("movieId").alias("movie_id"),
        col("rating")
    )
    
    ratings_df.write \
    .option("url", redshift_url) \
    .option("dbtable", "movies.stage_rating") \
    .option("tempformat", "CSV") \
    .option("ec2-master", "redshift-default") \
    .jdbc(url=redshift_url, table="movies.stage_rating", mode="append", properties=redshift_properties) \
    .save()

    ratings_df.write.parquet(f"{output}/rating.parquet")

def main():
    """
    This is the main thread that create the Spark instance (Session), reads AWS credentials and calls process_data.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/source")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()


    spark = create_spark_session()

    
    process_data(spark, file_input=args.input, output=args.output)
    
if __name__ == "__main__":
    main()