from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the Schema for the JSON data
rides_schema = StructType([StructField('base_fare', DoubleType(), True), StructField('booking_timestamp', StringType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('confirmation_number', StringType(), True), StructField('distance_fare', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('driver_id', StringType(), True), StructField('driver_license', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_phone', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_city_id', LongType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_location_id', StringType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('duration_minutes', LongType(), True), StructField('license_plate', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_id', StringType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('payment_method_id', LongType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_city_id', LongType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_location_id', StringType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('pickup_timestamp', StringType(), True), StructField('rating', LongType(), True), StructField('ride_id', StringType(), True), StructField('ride_status_id', LongType(), True), StructField('subtotal', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('vehicle_color', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('vehicle_make_id', LongType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_type_id', LongType(), True)])

# Empty Staging Table, acts as unified destination for bulk and live streaming
dp.create_streaming_table("stg_rides")

# Bulk/Initial Load Apped Flow
@dp.append_flow(
    target = "stg_rides",
    name = "append_bulk_history"
)
def rides_bulk():
    bulk_df = spark.readStream.table("bulk_rides")

    # NEW: Cast the string to a true timestamp
    bulk_df = bulk_df.withColumn("booking_timestamp", to_timestamp(col("booking_timestamp")))
    return bulk_df

# The Live Streaming Append Flow
@dp.append_flow(
    target = "stg_rides",
    name = "append_live_stream"
)
def rides_stream():
    stream_df = spark.readStream.table("bronze_uber_rides")
    # Unpack the raw JSON string from the 'rides' column into proper columns
    parsed_df = stream_df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))\
                            .select("parsed_rides.*")
    # NEW: Cast the string to a true timestamp here as well
    parsed_df = parsed_df.withColumn("booking_timestamp", to_timestamp(col("booking_timestamp")))
    return parsed_df
