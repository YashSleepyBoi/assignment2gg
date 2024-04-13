import sys
from pyspark.sql import SparkSession
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

input_file_name = '/content/TA_restaurants_curated_cleaned.csv'
output_path='/content/output'
df = spark.read.csv(input_file_name, header=True)


cleaned_df = df.filter(df["Number of Reviews"].isNotNull() & (df["Rating"] >= 1.0))


cleaned_df.write.mode('overwrite').option("header", True).csv(output_path)


cleaned_df.show()

spark.stop()