from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("AdditionExample").getOrCreate()

# Create a DataFrame with a single row and column
data = [(1,)]
df = spark.createDataFrame(data, ["value"])

# Perform the addition
result_df = df.select(df.value + 1)

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()
