import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
# you may add more import if you need to
hdfs_nn = sys.argv[1]
# Start session
spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
input_file_name = 'hdfs://%s:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv'%(hdfs_nn)
output_dir_name = 'hdfs://%s:9000/assignment2/output/question3'%(hdfs_nn)
df = spark.read.csv(input_file_name, header=True)

# Convert relevant columns to numerical types
df = df.withColumn("Rating", df["Rating"].cast("double"))

# Calculate average rating
average_rating_df = df.groupBy("City").avg("Rating")
average_rating_df = average_rating_df.withColumnRenamed("avg(Rating)", "AverageRating")

# Sort the DataFrame based on average rating and add RatingGroup column
top_avg_df = average_rating_df.orderBy("AverageRating", ascending=False).limit(3).withColumn("RatingGroup", lit("Top"))
low_avg_df = average_rating_df.orderBy("AverageRating").limit(3).withColumn("RatingGroup", lit("Bottom"))
combined_df = top_avg_df.union(low_avg_df)

# Write the output as CSV
combined_df.write.mode('overwrite').option("header", True).csv(output_dir_name)

# Stop SparkSession
spark.stop()
