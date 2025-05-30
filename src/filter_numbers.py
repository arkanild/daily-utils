from pyspark.sql.functions import explode, split, lower , col, trim
from typing import List
from pyspark.sql import DataFrame

def filter_even_numbers(spark, input_data):
    rdd = spark.sparkContext.parallelize(input_data)
    return rdd.filter(lambda x:x%2==0).collect()


def top_n_words(df:DataFrame, col_name:str, n:int, stopwords:List[str])-> List[str]:
        
        words_df = df.select(explode(split(lower(col(col_name))," ")).alias("word"))
        if stopwords:
            words_df = words_df.filter(~col("word").isin(stopwords))
        
        top_words = (
        words_df
            .groupBy("word")
            .count()
            .orderBy(col("count").desc(), col("word").asc())
            .limit(n)
        )
        return top_words.rdd.map(lambda row:row["word"]).collect()

#function to remove null or empty values in a dataframe
def remove_null_or_empty(df: DataFrame, col_name: str) -> DataFrame:
    return df.filter(
        col(col_name).isNotNull() & trim(col(col_name)).notEqual("")
    )


#function to explode a column to rows
def explode_to_rows(df:DataFrame,array_col:str) -> DataFrame:
     df.printSchema
     return df.select(col("user_id"),explode(col(array_col)).alias("products"))

