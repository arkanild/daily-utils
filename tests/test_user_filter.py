def test_filter_active_users(spark):
    from src.user_processing import filter_active_users
    input_data = [
        ("u1","active"),
        ("u2","inactive"),
        ("u3","active")]
    input_df = spark.createDataFrame(input_data,["user_id","status"])
    result_df= filter_active_users(input_df)
    result=[row["user_id"] for row in result_df.collect()]

    assert sorted(result) == ["u1","u3"]


def test_filter_even_numbers(spark):
    from src.filter_numbers import filter_even_numbers
    input_data = list(range(1,11))
    result_data = [2,4,6,8,10]

    result = filter_even_numbers(spark,input_data)
    assert sorted(result) == result_data

