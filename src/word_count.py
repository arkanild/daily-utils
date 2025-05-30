from pyspark.sql.functions import col, explode, split, lower
from pyspark.sql import DataFrame
from typing import List


def word_count(df:DataFrame, input_path:str, output_path:str)-> None:
    """
    Function to count words in a DataFrame and save the results to a file.""" 
    # TO DO: 
    # read the input file into a Dataframe
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    input_df = spark.read.csv(input_path, header=False, inferSchema=True)
    filtered_df  = input_df.withColumn("words", explode(split(lower(col("value"))," "))) 
                #.filter(~col("words").isNotNull() & col("words") != "")) #extra check 
    #TO DO:
    # remove stopwords if any 
    # stopwords = ["from", "the", "of", "and"] #example stopwords
    #count the words
    word_count = filtered_df.groupBy("words").count().orderBy(col("words").asc())

    # save the results to the a file in the output path
    word_count.write.csv(output_path, header=True)
    return word_count

