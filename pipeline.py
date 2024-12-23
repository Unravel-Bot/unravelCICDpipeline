from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit
import time
from pyspark.pandas import DataFrame as PandasOnSparkDF

# Define the parameters
numRows = 1000000  # Increase the number of rows to make the operation more expensive
numPartitions = 10  # Set a lower number of partitions to increase processing time

# Enabling Hive support to connect with the Hive metastore
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# Create a large DataFrame with a skewed distribution
df = spark.range(0, numRows, 1, numPartitions).selectExpr("id", "(id % 100) as sym")

# Create additional columns to increase complexity
df = df.withColumn("col1", expr("case when sym % 2 = 0 then sym else null end"))
df = df.withColumn("col2", expr("case when sym % 3 = 0 then sym else null end"))
df = df.withColumn("col3", expr("col1 * col2"))
df.createOrReplaceGlobalTempView("t1")

# Execute a complex SQL query that includes multiple joins and aggregations
df1 = spark.sql("""
    SELECT 
        COUNT(DISTINCT col3) AS distinct_col3_count, 
        SUM(col1) AS sum_col1, 
        AVG(col2) AS avg_col2, 
        sym 
    FROM 
        global_temp.t1 
    GROUP BY 
        sym 
    ORDER BY 
        sum_col1 DESC
""")

# Simulate a long-running operation within each partition
def process_partition(partition):
    time.sleep(10)  # Increase sleep time to simulate a slow operation
    return partition

df1.rdd.foreachPartition(process_partition)

df1.show()

# Convert to Pandas DataFrame
pandas_df = PandasOnSparkDF(df1)

# Get the number of rows in the Pandas DataFrame
rows_count = pandas_df.shape[0]
print(f"Number of rows: {rows_count}")
