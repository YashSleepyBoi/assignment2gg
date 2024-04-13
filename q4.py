import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace
# don't change this line
hdfs_nn = sys.argv[1]

# Start session
spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()

input_file_name = '/content/TA_restaurants_curated_cleaned.csv'
output_path = '/content/output3'
df = spark.read.csv(input_file_name, header=True)
# Extract cuisine style


df = df.withColumn("Cuisine Style", split(col("Cuisine Style"), "', '"))

# Explode the array to have one row per cuisine
df = df.withColumn("Cuisine", explode("Cuisine Style"))
df = df.withColumn("Cuisine", regexp_replace(df["Cuisine"], "\s*' \]$", ""))
df = df.withColumn("Cuisine", regexp_replace(df["Cuisine"], "^\[ '\s*", ""))

# Group by city and cuisine, then count the number of restaurants
restaurant_counts_df = df.groupBy("City", "Cuisine").count().orderBy("City","Cuisine")

# Write the output as CSV
restaurant_counts_df.write.mode('overwrite').option("header", True).csv(output_path)

restaurant_counts_df.show()

spark.stop()