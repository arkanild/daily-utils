from pyspark.sql import DataFrame ,SparkSession
import pyspark.sql.functions as F

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3 # 6371 Kms  # 6371 * 0.62 ~= 3958 miles
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE
METERS_TO_MILES = 0.000621371


def compute_distance(spark, dataframe: DataFrame)->DataFrame:
    
    start_latitude = 'start station latitude'
    end_latitude = 'end station latitude'
    start_longitude = 'start station longitude'
    end_longitude = 'end station longitude'

    calculated_distance = EARTH_RADIUS_IN_METERS * 2 * F.atan2(
        F.sqrt(
            F.pow(F.sin((F.radians(F.col(end_latitude) - F.col(start_latitude)) / 2), 2) +
            F.cos(F.radians(F.col(start_latitude))) *
            F.cos(F.radians(F.col(end_latitude))) *
            F.pow(F.sin((F.radians(F.col(end_longitude) - F.col(start_longitude)) / 2), 2))
        ),
        F.sqrt(
            1 - (
                F.pow(F.sin((F.radians(F.col(end_latitude) - F.col(start_latitude)) / 2), 2) +
                F.cos(F.radians(F.col(start_latitude))) *
                F.cos(F.radians(F.col(end_latitude))) *
                F.pow(F.sin((F.radians(F.col(end_longitude) - F.col(start_longitude)) / 2), 2))
                    )       
                )
        ) * METERS_TO_MILES
        )
    )
    return dataframe.select("distance", calculated_distance) 

def transforms_calculate_distance(spark, input_dataset_path:str, output_dataset_path:str)-> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_w_distace = compute_distance(spark, input_dataset)
    dataset_w_distace.show()

    dataset_w_distace.write.parquet(output_dataset_path,mode='append')