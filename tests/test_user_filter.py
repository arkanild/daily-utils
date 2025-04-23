def test_filter_active_users(spark):
    from user_processing import filter_active_users
    input_data = [
        ("u1",active)
        ("u2", inactive)
        ("u3", active)
    ]
    input_df = spark.createDataframe(input_data,["user_id","status"])
    result_df= filter_active_users(input_df)
    result=[row["user"] for row in result_df.collect()]

    assert sorted(result) == ["u1","u3"]