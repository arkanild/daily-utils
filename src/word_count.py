from pyspark.sql.functions import col, explode, split, lower
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import logging

def word_count(df:DataFrame, input_path:str, output_path:str)-> None:
    """
    Function to count words in a DataFrame and save the results to a file.""" 
    # TO DO: 
    # create a spark session 
    # read the input file into a dataframe
    # lower, split, explode the column valie or _c0 into column words using alias
    # groupby, count, orderby according to the test case results
    # coalesce the word count df into one parition and write it into a output path as a csv
    
    spark = SparkSession.builder.getOrCreate()

    input_df = spark.read.text(input_path)
    filtered_df  = input_df.select(explode(split(lower(input_df.value))," ")).alias("words")
                #.filter(~col("words").isNotNull() & col("words") != "")) #extra check 
    word_count_df = filtered_df.groupBy("words").count().orderBy(col("words").asc())

    # save the results to the a file in the output path
    logging.info("Writing csv to directory: %s", output_path)
    word_count_df.coalesce(1).write.csv(output_path, header=True)
    

def word_count_exclude_stopwords(spark, input_path, output_path)-> None:

    stopwords = ["a", "an", "the", "in"]

    input_df = spark.read.text(input_path)
    words_df = input_df.select(explode(split(lower(input_df.value),"[\\s.,;!?:\"-]+")).alias("word"))
    words_df = words_df.filter(col("word") !="")

    logging.info("Filter if stopwords are present")
    if stopwords:
        words_df = words_df.filter(~col("word").isin(stopwords))

    word_count_df = words_df.groupBy("word").count().orderBy(col("word").asc())

    logging.info("Writing csv into output directory:%s", output_path)
    word_count_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")


