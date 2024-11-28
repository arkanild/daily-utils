# daily-utils
A utility library to automate data engineering tasks like scrapping , formatting and extracting data. 

#prerequisites for running the pyspark code
pip install pyspark
pip install google-cloud-bigquery

#spark-bigquery connector
wget https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.29.0/spark-bigquery-with-dependencies_2.12-0.29.0.jar

#spark-submit command 
spark-submit --jars spark-bigquery-with-dependencies_2.12-0.29.0.jar your_script.py

# prerequisites to run airflow code 
pip install apache-airflow[gcp]
pip install requests
