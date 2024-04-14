import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace
# don't change this line
hdfs_nn = sys.argv[1]

# Start session
spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()

input_file_name = 'hdfs://%s:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv'%(hdfs_nn)
output_dir_name = 'hdfs://%s:9000/assignment2/output/question4'%(hdfs_nn)
df = spark.read.csv(input_file_name, header=True)
# Extract cuisine style


df = df.withColumn("Cuisine Style", split(col("Cuisine Style"), "', '"))

# Explode the array to have one row per cuisine
df = df.withColumn("Cuisine", explode("Cuisine Style"))
df = df.withColumn("Cuisine", regexp_replace(df["Cuisine"], "\s*' \]$", ""))
df = df.withColumn("Cuisine", regexp_replace(df["Cuisine"], "^\[ '\s*", ""))

# Group by city and cuisine, then count the number of restaurants
restaurant_counts_df = df.groupBy("City", "Cuisine").count().orderBy("City","Cuisine")
restaurant_counts_df.show()

# Write the output as CSV
restaurant_counts_df.write.mode('overwrite').option("header", True).csv(output_dir_name)


spark.stop()