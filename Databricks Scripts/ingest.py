from pyspark import pipelines as dp  # The new Lakeflow Declarative standard
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Your Event Hubs configuration
EH_NAMESPACE = "uber-stream-nafis" 
EH_NAME = "uber-topic"
# Paste your actual SharedAccessKey below
EH_CONN_STR = "ListenPolicy;SharedAccessKey for uber-topic"

# 2. Kafka Options (with the dictionary bug fixes applied!)
kafka_options = {
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.sasl.jaas.config"   : f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
  "kafka.request.timeout.ms" : "60000",
  "kafka.session.timeout.ms" : "60000",
  "maxOffsetsPerTrigger"     : "10000",
  "failOnDataLoss"           : "false",
  "startingOffsets"          : "earliest"
}

# 3. The DLT Decorator
@dp.table(
  name="bronze_uber_rides",
  comment="Raw Uber streaming data ingested from Event Hubs",
  table_properties={"quality": "bronze"}
)
def ingest_uber_stream():
    # 4. Read the stream (Notice we explicitly chain the server and protocol first)
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", f"{EH_NAMESPACE}.servicebus.windows.net:9093")
          .option("kafka.security.protocol", "SASL_SSL")
          .options(**kafka_options)
          .load())
    
    # 5. Converting the Values to String
    df = df.withColumn("rides", col("value").cast(StringType()))

    # 6. Return the dataframe. SDP handles the writing and checkpointing automatically!
    return df
