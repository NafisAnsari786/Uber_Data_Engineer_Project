import dlt
from pyspark.sql.functions import col

# ==========================================
# 1. DIM_PASSENGER (SCD TYPE 1)
# ==========================================
@dlt.view(name="dim_passenger_view")
def dim_passenger_view():
    return spark.readStream \
        .option("skipChangeCommits", "true") \
        .option("ignoreChanges", "true") \
        .table("LIVE.obt_rides").select(
            "passenger_id", "passenger_name", "passenger_email", "passenger_phone", "booking_timestamp"
        )

dlt.create_streaming_table("dim_passenger")

dlt.apply_changes(
    target = "dim_passenger",
    source = "dim_passenger_view",
    keys = ["passenger_id"],
    sequence_by = col("booking_timestamp"), 
    stored_as_scd_type = 1
)

# ==========================================
# 2. DIM_DRIVER (SCD TYPE 1)
# ==========================================
@dlt.view(name="dim_driver_view")
def dim_driver_view():
    return spark.readStream \
        .option("skipChangeCommits", "true") \
        .option("ignoreChanges", "true") \
        .table("LIVE.obt_rides").select(
            "driver_id", "driver_name", "driver_license", "driver_phone", "driver_rating", "booking_timestamp"
        )

dlt.create_streaming_table("dim_driver")

dlt.apply_changes(
    target = "dim_driver",
    source = "dim_driver_view",
    keys = ["driver_id"],
    sequence_by = col("booking_timestamp"),
    stored_as_scd_type = 1
)

# ==========================================
# 3. DIM_VEHICLE (SCD TYPE 1)
# ==========================================
@dlt.view(name="dim_vehicle_view")
def dim_vehicle_view():
    return spark.readStream \
        .option("skipChangeCommits", "true") \
        .option("ignoreChanges", "true") \
        .table("LIVE.obt_rides").select(
            "vehicle_id", "vehicle_make_id", "vehicle_type_id", "vehicle_model", 
            "vehicle_color", "license_plate", "vehicle_make", "vehicle_type", "booking_timestamp"
        )

dlt.create_streaming_table("dim_vehicle")

dlt.apply_changes(
    target = "dim_vehicle",
    source = "dim_vehicle_view",
    keys = ["vehicle_id"],
    sequence_by = col("booking_timestamp"),
    stored_as_scd_type = 1
)

# ==========================================
# 4. DIM_PAYMENT (SCD TYPE 1)
# ==========================================
@dlt.view(name="dim_payment_view")
def dim_payment_view():
    return spark.readStream \
        .option("skipChangeCommits", "true") \
        .option("ignoreChanges", "true") \
        .table("LIVE.obt_rides").select(
            "payment_method_id", "payment_method", "is_card", "requires_auth", "booking_timestamp"
        )

dlt.create_streaming_table("dim_payment")

dlt.apply_changes(
    target = "dim_payment",
    source = "dim_payment_view",
    keys = ["payment_method_id"],
    sequence_by = col("booking_timestamp"),
    stored_as_scd_type = 1
)


# ==========================================
# 5. DIM_BOOKING (SCD TYPE 1)
# ==========================================
@dlt.view(name="dim_booking_view")
def dim_booking_view():
    return spark.readStream \
        .option("skipChangeCommits", "true") \
        .option("ignoreChanges", "true") \
        .table("LIVE.obt_rides").select(
            "ride_id", "confirmation_number", "ride_status_id", "ride_status", 
            "cancellation_reason_id", "cancellation_reason", "booking_timestamp", 
            "pickup_timestamp", "dropoff_timestamp"
        )

dlt.create_streaming_table("dim_booking")

dlt.apply_changes(
    target = "dim_booking",
    source = "dim_booking_view",
    keys = ["ride_id"],
    sequence_by = col("booking_timestamp"),
    stored_as_scd_type = 1
)


# ==========================================
# 6. DIM_LOCATION (SCD TYPE 2)
# ==========================================
@dlt.view(name="dim_location_view")
def dim_location_view():
    return spark.readStream \
        .option("ignoreChanges", "true") \
        .table("LIVE.obt_rides").select(
            "pickup_city_id", "pickup_city_name", "pickup_city_updated_at", "pickup_region", "pickup_state"
        )

dlt.create_streaming_table("dim_location")

dlt.apply_changes(
    target = "dim_location",
    source = "dim_location_view",
    keys = ["pickup_city_id"], 
    sequence_by = col("pickup_city_updated_at"), 
    stored_as_scd_type = 2
)


# ==========================================
# 7. FACT_RIDES (SCD TYPE 1)
# ==========================================
@dlt.view(name="fact_view")
def fact_view():
    # Only Foreign Keys and Metrics belong here!
    return spark.readStream \
        .option("skipChangeCommits", "true") \
        .option("ignoreChanges", "true") \
        .table("LIVE.obt_rides").select(
            "ride_id", "pickup_location_id", "dropoff_location_id", "payment_method_id", 
            "driver_id", "passenger_id", "vehicle_id", "distance_miles", 
            "duration_minutes", "base_fare", "distance_fare", "time_fare", 
            "surge_multiplier", "subtotal", "tip_amount", "total_fare", "rating", 
            "booking_timestamp"
        )

dlt.create_streaming_table("fact_rides")

dlt.apply_changes(
    target = "fact_rides",
    source = "fact_view",
    keys = ["ride_id"],
    sequence_by = col("booking_timestamp"),
    stored_as_scd_type = 1
)