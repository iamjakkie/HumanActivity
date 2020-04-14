from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, minute, second
import pyspark.sql.functions as F
from pyspark.sql.types import *

input_data = "s3a://s3emrdendudacity/raw/"
output_data = "s3a://s3emrdendudacity/humanactivity/"

glasses_df = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .option("inferschema", "true") \
    .load("{}glasses.csv" \
          .format(input_data))

report_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferschema", "true") \
    .load("{}report.csv" \
          .format(input_data))

smartphone_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferschema", "true") \
    .load("{}smartphone.csv" \
          .format(input_data))

smartwatch_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferschema", "true") \
    .load("{}smartwatch.csv" \
          .format(input_data))

# Fix timestamp columns
glasses_df = glasses_df.withColumn("timestamp", F.to_timestamp(glasses_df['DATE'], 'yyyy/MM/dd HH:mm:ss'))
report_df = report_df.withColumn("timestamp", F.to_timestamp(report_df["from"], 'yyyy-MM-dd HH:mm'))
smartphone_df = smartphone_df.withColumn("timestamp", F.to_timestamp(smartphone_df["timestamp"], 'yyyy-MM-dd HH:mm:ss'))
smartwatch_df = smartwatch_df.withColumn("timestamp", F.to_timestamp(smartwatch_df["timestamp"], 'yyyy-MM-dd HH:mm:ss'))

# Select only required data
source_glasses = glasses_df.select(["timestamp", "ACC_X", "ACC_Y", "ACC_Z", "GYRO_X",
                                    "GYRO_Y", "GYRO_Z", "EOG_L", "EOG_R", "EOG_H", "EOG_V"])
get_hours = udf(lambda str: int(str.split(':')[0]), IntegerType())
get_minutes = udf(lambda str: int(str.split(':')[1]), IntegerType())
report_df = report_df.withColumn("hourDuration", get_hours(report_df.duration)).withColumn("minuteDuration",
                                                                                           get_minutes(
                                                                                               report_df.duration))
source_report = report_df.select(["timestamp", "activity_type", "hourDuration", "minuteDuration"])
source_smartphone = smartphone_df \
    .select(["timestamp", "source", "values"]) \
    .withColumnRenamed("source", "phoneSource") \
    .withColumnRenamed("values", "phoneValues")
source_smartwatch = smartwatch_df.select(["timestamp", "source", "values"]) \
    .withColumnRenamed("source", "watchSource") \
    .withColumnRenamed("values", "watchValues")

# Combine all data
combined = source_glasses \
    .join(source_report, "timestamp", how="full") \
    .join(source_smartphone, "timestamp", how="full") \
    .join(source_smartwatch, "timestamp", how="full") \
    .orderBy("timestamp")

# Create dim_People feed (manual)
people_df = spark.createDataFrame([(1, "TestUser")], ["Id", "Name"])

# Create dim_Sources feed (Manual)
sourceList = [(1, "smartphone"), (2, "smartwatch"), (3, "glasses")]
sources_df = spark.createDataFrame(sourceList, ["Id", "Name"])
sources_df = sources_df.withColumnRenamed("value", "source")

# Exclude data with array as value - part of these will be split
excluded = ['wifi', 'orientation', 'magnetometer', 'audio', 'rotationVector', 'rotationVector', 'linear_acceleration',
            'gyroscope', 'accelerometer', 'rotation_vector', 'activity', 'bluetooth']

# Get all collectibles from phone
phoneCollectibles = combined.where(~combined.phoneSource.isin(excluded)).select("phoneSource").distinct()
# Get all collectibles from watch
watchCollectibles = combined.where(~combined.watchSource.isin(excluded)).select("watchSource").distinct()
# Create collectible list from glasses
glassList = ["ACC_X", "ACC_Y", "ACC_Z", "GYRO_X", "GYRO_Y", "GYRO_Z", "EOG_L", "EOG_R", "EOG_H", "EOG_V"]
glassCollectibles = spark.createDataFrame(glassList, StringType())
glassCollectibles = glassCollectibles.withColumnRenamed("value", "glassSource")

# Create dim_Collectibles feed
collectibles_df = phoneCollectibles.union(watchCollectibles) \
    .union(glassCollectibles) \
    .distinct() \
    .withColumnRenamed("phoneSource", "Collectible")

# Generate ID column
collectibles_df.createOrReplaceTempView("collectibles")
collectibles_df = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY Collectible) as Id, * FROM collectibles")

# Generate dim_Time feed
combined_timestamp = source_glasses.select("timestamp") \
    .union(source_report.select("timestamp")) \
    .union(source_smartphone.select("timestamp")) \
    .union(source_smartwatch.select("timestamp"))
time_df = combined_timestamp.select("timestamp") \
    .where(col("Timestamp").isNotNull()) \
    .distinct() \
    .orderBy("timestamp")
time_df = time_df.withColumn("Year", year(time_df["timestamp"])) \
    .withColumn("Month", month(time_df["timestamp"])) \
    .withColumn("Day", dayofmonth(time_df["timestamp"])) \
    .withColumn("Hour", hour(time_df["timestamp"])) \
    .withColumn("Minute", minute(time_df["timestamp"])) \
    .withColumn("Second", second(time_df["timestamp"]))

# prepare glasses activities
glasses_activities_acc_x = time_df.join(glasses_df, "timestamp", how="inner") \
    .select(
    [F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("ACC_X").alias("Collectible"), "timestamp", "ACC_X"])
glasses_activities_acc_y = time_df.join(glasses_df, "timestamp", how="inner") \
    .select(
    [F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("ACC_Y").alias("Collectible"), "timestamp", "ACC_Y"])
glasses_activities_acc_z = time_df.join(glasses_df, "timestamp", how="inner") \
    .select(
    [F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("ACC_Z").alias("Collectible"), "timestamp", "ACC_Z"])
glasses_activities_gyro_x = time_df.join(glasses_df, "timestamp", how="inner") \
    .select([F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("GYRO_X").alias("Collectible"), "timestamp",
             "GYRO_X"])
glasses_activities_gyro_y = time_df.join(glasses_df, "timestamp", how="inner") \
    .select([F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("GYRO_Y").alias("Collectible"), "timestamp",
             "GYRO_Y"])
glasses_activities_gyro_z = time_df.join(glasses_df, "timestamp", how="inner") \
    .select([F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("GYRO_Z").alias("Collectible"), "timestamp",
             "GYRO_Z"])
glasses_activities_eog_l = time_df.join(glasses_df, "timestamp", how="inner") \
    .select(
    [F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("EOG_L").alias("Collectible"), "timestamp", "EOG_L"])
glasses_activities_eog_r = time_df.join(glasses_df, "timestamp", how="inner") \
    .select(
    [F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("EOG_R").alias("Collectible"), "timestamp", "EOG_R"])
glasses_activities_eog_h = time_df.join(glasses_df, "timestamp", how="inner") \
    .select(
    [F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("EOG_H").alias("Collectible"), "timestamp", "EOG_H"])
glasses_activities_eog_v = time_df.join(glasses_df, "timestamp", how="inner") \
    .select(
    [F.lit(1).alias("PersonId"), F.lit(3).alias("SourceId"), F.lit("EOG_V").alias("Collectible"), "timestamp", "EOG_V"])

glasses_activities = glasses_activities_acc_x.union(glasses_activities_acc_y) \
    .union(glasses_activities_acc_z) \
    .union(glasses_activities_gyro_x) \
    .union(glasses_activities_gyro_y) \
    .union(glasses_activities_gyro_z) \
    .union(glasses_activities_eog_l) \
    .union(glasses_activities_eog_r) \
    .union(glasses_activities_eog_h) \
    .union(glasses_activities_eog_v) \
    .withColumnRenamed("ACC_X", "value")

# Prepare smartphone activities
smartphone_activities = source_smartphone.select(
    ([F.lit(1).alias("PersonId"), F.lit(1).alias("SourceId"), "phoneSource", "timestamp", "phoneValues"]))
# Prepare smartwatch activities
smartwatch_activities = source_smartwatch.select(
    ([F.lit(1).alias("PersonId"), F.lit(2).alias("SourceId"), "watchSource", "timestamp", "watchValues"]))
# union all activities
activities_sources = glasses_activities.union(smartphone_activities) \
    .union(smartwatch_activities) \
    .orderBy("timestamp")
# Create activities fact feed
activities = activities_sources.alias("as").join(source_report.alias("sr"), "timestamp", how="outer") \
    .join(collectibles_df.alias("cdf"), "Collectible", how="inner") \
    .select("as.PersonId", "as.SourceId", col("cdf.Id").alias("CollectibleId"), "timestamp", "as.value") \
    .orderBy("timestamp")

# save parquets
people_df.write.mode("overwrite").parquet("{}people".format(output_data))
sources_df.write.mode("overwrite").parquet("{}sources".format(output_data))
collectibles_df.write.mode("overwrite").parquet("{}collectibles".format(output_data))
time_df.write.mode("overwrite").parquet("{}time".format(output_data))
activities.write.mode("overwrite").parquet("{}activities".format(output_data))

