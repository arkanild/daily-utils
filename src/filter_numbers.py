from pyspark.sql.functions import explode, split, lower , col, trim, count, sum as _sum
from typing import List
from pyspark.sql import DataFrame

#function to filter even numbers
def filter_even_numbers(spark, input_data):
    rdd = spark.sparkContext.parallelize(input_data)
    return rdd.filter(lambda x:x%2==0).collect()


#function to find the top n words from a dataframe
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
     #df.printSchema
     return df.select(col("user_id"),explode(col(array_col)).alias("products"))


#function to check the most frequent customers 
def filter_frequent_customer(df,user_id_col, purchase_amount_col)-> DataFrame:
    from pyspark.sql import functions as F
    agg_df = (
        df.groupBy(user_id_col)
        .agg(F.count("*").alias("txn_count"),
             F.sum(purchase_amount_col).alias("total_purchase")       
            )
            )
    result_df = (
    agg_df.filter((F.col("txn_count")>3) & (F.col("total_purchase")>1000))
                        .select(F.col(user_id_col))
                        .orderBy(F.col(user_id_col).asc())
     
    )
    return result_df

#helper function to load data and call the function.
def _run_frequent_filter(spark, input_path:str)->DataFrame:
     input_df =spark.read.csv(input_path, header=True, inferSchema=True)
     return filter_frequent_customer(input_df,"user_id", "purchase_amount")

#main function 
if __name__ == "__main__":
     from pyspark.sql import SparkSession
     spark = SparkSession.builder.master("local[*]").appName("FrequentCustomerFilter").getOrCreate()
     input_path = f"/resources/customer_data.txt"
     result_df = _run_frequent_filter(spark,input_path)
     result_df.show()


     