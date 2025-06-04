from pyspark.sql import DataFrame ,SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3 # 6371 Kms  # 6371 * 0.62 ~= 3958 miles
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE
METERS_TO_MILES = 0.000621371


# def compute_distance(spark, dataframe: DataFrame)->DataFrame:
    
#     start_latitude = 'start_station_latitude'
#     end_latitude = 'end_station_latitude'
#     start_longitude = 'start_station_longitude'
#     end_longitude = 'end_station_longitude'

#     calculated_distance = (
#     EARTH_RADIUS_IN_METERS * 2 * F.atan2(
#         F.sqrt(
#             # Corrected: F.pow(F.sin(value), 2)
#             F.pow(F.sin(F.radians(F.col(end_latitude) - F.col(start_latitude)) / 2), 2) +
#             F.cos(F.radians(F.col(start_latitude))) *
#             F.cos(F.radians(F.col(end_latitude))) *
#             # Corrected: F.pow(F.sin(value), 2)
#             F.pow(F.sin(F.radians(F.col(end_longitude) - F.col(start_longitude)) / 2), 2)
#         ),
#         F.sqrt(
#             1 - (
#                 # Corrected: F.pow(F.sin(value), 2)
#                 F.pow(F.sin(F.radians(F.col(end_latitude) - F.col(start_latitude)) / 2), 2) +
#                 F.cos(F.radians(F.col(start_latitude))) *
#                 F.cos(F.radians(F.col(end_latitude))) *
#                 # Corrected: F.pow(F.sin(value), 2)
#                 F.pow(F.sin(F.radians(F.col(end_longitude) - F.col(start_longitude)) / 2), 2)
#                 )
#             )
#             ) * METERS_PER_MILE
#         )   
#     return dataframe.withColumn("distance", calculated_distance) 

# oversimplified version 

def compute_distance(_spark:SparkSession, dataframe:DataFrame)-> DataFrame:

    #Filter out the required columns
    start_lat = 'start_station_latitude'
    end_lat = 'end_station_latitude'
    start_long = 'start_station_longitude'
    end_long = 'end_station_longitude'

    #convert into double
    dataframe = dataframe.withColumn(start_lat, F.col(start_lat).cast(DoubleType())) \
                         .withColumn(end_lat, F.col(end_lat).cast(DoubleType())) \
                         .withColumn(start_long, F.col(start_long).cast(DoubleType())) \
                         .withColumn(end_long,F.col(end_long.cast(DoubleType())))

    #calculate distance
     #convert degrees into radians for trigonometric functions
    lat1_rad = F.radians(F.col(start_lat))
    long1_rad = F.radians(F.col(start_long))
    lat2_rad = F.radians(F.col(end_lat))
    long2_rad = F.radians(F.col(end_long))

    #finding the delta in coordinates
    dlat = lat2_rad - lat1_rad
    dlong = long2_rad - long1_rad

    #Haversine formula components 
    a = F.pow(F.sin(dlat/2),2)+ F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(dlong/2),2)

    c =  2 * F.atan2(F.sqrt(a),F.sqrt(1-a))

    #calculate distance in meters
    calculated_distance_in_meters = EARTH_RADIUS_IN_METERS * c
    calculated_distance_in_miles = calculated_distance_in_meters * (1/METERS_PER_MILE) #Use 1/METERS_PER_MILE to convert meters to miles

    #append the distance column with the dataframe
    return dataframe.withColumn('Distance',calculated_distance_in_miles)


def transforms_calculate_distance(spark, input_dataset_path:str, output_dataset_path:str)-> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_w_distace = compute_distance(spark, input_dataset)
    dataset_w_distace.show()

    dataset_w_distace.write.parquet(output_dataset_path,mode='append')