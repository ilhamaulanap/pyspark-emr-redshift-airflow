import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl,
                               IntegerType as Int, DateType as Date,
                               BooleanType as Boolean, FloatType as Float,
                               LongType as Long, StringType as String,
                               ArrayType as Array)
from pyspark.sql.functions import (col, year, month, dayofmonth, weekofyear, quarter,
                                   explode, from_json)


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

    movies_schema = StructType([
    Fld("adult", String()),
    Fld("belongs_to_collection", Long()),
    Fld("budget", Long()),
    Fld("genres", String()),
    Fld("homepage", String()),
    Fld("id", Int()),
    Fld("imdb_id", String()),
    Fld("original_language", String()),
    Fld("original_title", String()),
    Fld("overview", String()),
    Fld("popularity", Dbl()),
    Fld("poster_path", String()),
    Fld("production_company", String()),
    Fld("production_country",  String()),
    Fld("release_date", Date()),
    Fld("revenue", Long()),
    Fld("runtime", Float()),
    Fld("spoken_languages", String()),
    Fld("status", String()),
    Fld("tagline", String()),
    Fld("title", String()),
    Fld("video", Boolean()),
    Fld("vote_average", Float()),
    Fld("vote_count", Int())
])  

    movies_df = spark.read.option("header", "true") \
                            .csv(f"{file_input}/movies_metadata.csv", 
                                schema=movies_schema)


    genre_schema = Array(StructType([Fld("id", Int()), Fld("name", String())]))

    movies_df = movies_df.withColumn("genres", explode(from_json("genres", genre_schema))) \
                         .withColumn("genre_id", col("genres.id")) \
                         .withColumn("genre_name", col("genres.name")) \
    
    movie_genre = movies_df.select("id", "genre_id").distinct()
    movie_genre = movie_genre.select(col("id").alias("movie_id"), col("genre_id"))
    
    genre = movies_df.select("genre_id", "genre_name").distinct()
    genre = genre.na.drop()
    

    #Load data into 
    genre.write \
    .option("url", redshift_url) \
    .option("dbtable", "movies.stage_genre") \
    .option("tempformat", "CSV") \
    .option("ec2-master", "redshift-default") \
    .jdbc(url=redshift_url, table="movies.stage_genre", mode="append", properties=redshift_properties) \
    .save()

    genre.write.parquet(f"{output}/genre.parquet")

    movie_genre.write \
    .option("url", redshift_url) \
    .option("dbtable", "movies.stage_movie_genre") \
    .option("tempformat", "CSV") \
    .option("ec2-master", "redshift-default") \
    .jdbc(url=redshift_url, table="movies.stage_movie_genre", mode="append", properties=redshift_properties) \
    .save()

    movie_genre.write.parquet(f"{output}/movie_genre.parquet")

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