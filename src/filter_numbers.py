def filter_even_numbers(spark, input_data):
    rdd = spark.sparkContext.parallelize(input_data)
    return rdd.filter(lambda x:x%2==0).collect()