import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_, min as min_
# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 3").getOrCreate()

input_fpath = '/content/TA_restaurants_curated_cleaned.csv'
df = spark.read.csv(input_fpath, header=True)

# Filter out rows with null Price Range
df_filtered = df.filter(col("Price Range").isNotNull())

# Group by city and price range, find the restaurant with the highest and lowest rating
result_df = df_filtered.groupby("City", "Price Range").

# Join with the original DataFrame to get the other columns
result_df = result_df.join(df_filtered, ["City", "Price Range"], "inner") \
    .select("City", "Price Range", "Name", "Cuisine Style", "Ranking", "Rating", "Number of Reviews", "Reviews", "URL_TA", "ID_TA", "Max Rating", "Min Rating")

result_df.show()

# Write the filtered DataFrame as CSV
result_df.write.csv("/assignment2/output/question2/", header=True)