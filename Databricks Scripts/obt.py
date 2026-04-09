# Jinja Template for OBT

import dlt
from jinja2 import Template

# 1. THE METADATA CONFIGURATION
# This list of dictionaries is the ONLY thing you change if you ever add or remove mapping tables in the future.
# Note: We use map_cities twice (for pickup and dropoff), so the 'alias' prevents SQL conflicts!
# We added "cols" to explicitly select and rename specific columns, avoiding duplicates.

mapping_config = [
    {
        "stg_col": "cancellation_reason_id", 
        "map_table": "map_cancellation_reasons", 
        "alias": "cancel_reason", 
        "map_id_col": "cancellation_reason_id",
        "cols": {"cancellation_reason": "cancellation_reason"}
    },
    {
        "stg_col": "pickup_city_id", 
        "map_table": "map_cities", 
        "alias": "pickup_city", 
        "map_id_col": "city_id",
        "cols": {"city": "pickup_city_name", "state": "pickup_state", "region": "pickup_region", "updated_at": "pickup_city_updated_at"}
    },
    {
        "stg_col": "dropoff_city_id", 
        "map_table": "map_cities", 
        "alias": "dropoff_city", 
        "map_id_col": "city_id",
        "cols": {"city": "dropoff_city_name", "state": "dropoff_state", "region": "dropoff_region", "updated_at": "dropoff_city_updated_at"}
    },
    {
        "stg_col": "payment_method_id", 
        "map_table": "map_payment_methods", 
        "alias": "payment", 
        "map_id_col": "payment_method_id",
        "cols": {"payment_method": "payment_method", "is_card": "is_card", "requires_auth": "requires_auth"}
    },
    {
        "stg_col": "ride_status_id", 
        "map_table": "map_ride_statuses", 
        "alias": "status", 
        "map_id_col": "ride_status_id",
        "cols": {"ride_status": "ride_status"}
    },
    {
        "stg_col": "vehicle_make_id", 
        "map_table": "map_vehicle_makes", 
        "alias": "vehicle_make", 
        "map_id_col": "vehicle_make_id",
        "cols": {"vehicle_make": "vehicle_make"}
    },
    {
        "stg_col": "vehicle_type_id", 
        "map_table": "map_vehicle_types", 
        "alias": "vehicle_type", 
        "map_id_col": "vehicle_type_id",
        "cols": {"vehicle_type": "vehicle_type"}
    }
]

# 2. THE JINJA SQL TEMPLATE
# We write the SQL structure once. Jinja will loop through the config above and write all the repetitive JOINs for us.

obt_sql_template = """
SELECT 
    stg.*
    
    -- Jinja Loop 1: Loop through our new 'cols' dictionary to rename columns
    {% for table in mappings %}
        {% for source_col, final_name in table.cols.items() %}
        , {{ table.alias }}.{{ source_col }} AS {{ final_name }}
        {% endfor %}
    {% endfor %}

FROM LIVE.stg_rides stg

-- Jinja Loop 2: The exact same LEFT JOIN logic as before
{% for table in mappings %}
LEFT JOIN LIVE.{{ table.map_table }} AS {{ table.alias }}
    ON stg.{{ table.stg_col }} = {{ table.alias }}.{{ table.map_id_col }}
{% endfor %}
"""

# 3. THE DLT EXECUTION
# This tells Databricks to create the final table by rendering our Jinja template and executing the resulting SQL.
@dlt.table(
    name="obt_rides",
    comment="One Big Table joining staging rides with all mapping dimensions without duplicate columns."
)
def create_obt():
    jinja_template = Template(obt_sql_template)
    rendered_sql = jinja_template.render(mappings=mapping_config)
    return spark.sql(rendered_sql)














