from pyspark.sql import SparkSession

# Initialize the Spark session
spark = SparkSession.builder \
    .appName('Spark GCS to BigQuery') \
    .config('spark.jars', 'spark-bigquery-with-dependencies_2.12-0.29.0.jar') \
    .getOrCreate()

# Path to your CSV file in Google Cloud Storage
csv_file_path = 'gs://your-bucket-name/path/to/your/csvfile.csv'

# Read the CSV file into a DataFrame
df = spark.read.format('csv').option('header', 'true').load(csv_file_path)

# Print the schema of the DataFrame to verify
df.printSchema()

# Configuration for writing to BigQuery
project_id = 'your-gcp-project-id'
dataset_id = 'your_bigquery_dataset'
table_id = 'your_bigquery_table'
temporary_bucket = 'your_temporary_gcs_bucket'

# Write the DataFrame to BigQuery
df.write.format('bigquery') \
    .option('table', f'{project_id}:{dataset_id}.{table_id}') \
    .option('temporaryGcsBucket', temporary_bucket) \
    .save()

# Stop the Spark session
spark.stop()
