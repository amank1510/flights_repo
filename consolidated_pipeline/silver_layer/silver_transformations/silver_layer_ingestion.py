import dlt
from pyspark.sql.functions import col,to_date,current_timestamp

#silver transformation for bookings

@dlt.view(
  name="bookings_view"
)
def bookings_view():
  df=spark.readStream.format("delta").load("/Volumes/flights/bronze/bronze_volume/bookings/data/")

  df_transformed=(
                  df.withColumn("modified_date",current_timestamp())
                    .withColumn("booking_date",to_date(col("booking_date"),"yyyy-MM-dd"))
                    .withColumn("amount",col("amount").cast("double"))
                    .drop("_rescued_data")
                  )
  return df_transformed

rules={                                                 #dictionary for expectation
  "rule1":"booking_id IS NOT NULL",
  "rule2":"passenger_id IS NOT NULL"
}

@dlt.table(
  name="silver_bookings",
  table_properties={"quality":"silver"},
  comment="Silver table for bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
  df=dlt.readStream("bookings_view")
  return df


#silver transformation for passengers

@dlt.view(
  name="passengers_view"
)
def passengers_view():
  df=spark.readStream.format("delta").load("/Volumes/flights/bronze/bronze_volume/passengers/data/")

  df_transformed=df.withColumn("modified_date",current_timestamp())

  return df_transformed

dlt.create_streaming_table(name="silver_passengers",
                           table_properties={"quality":"silver"},
                           comment="Silver table for passengers"
                           )

dlt.create_auto_cdc_flow(
  target="silver_passengers",
  source="passengers_view",
  keys=["passenger_id"],
  sequence_by=col("modified_date"),
  except_column_list=["_rescued_data"],
  stored_as_scd_type=1
)

#silver transformation for airports

@dlt.view(
  name="airports_view"
)
def airports_view():
  df=spark.readStream.format("delta").load("/Volumes/flights/bronze/bronze_volume/airports/data/")

  df_transformed=df.withColumn("modified_date",current_timestamp())

  return df_transformed

dlt.create_streaming_table(
  name="silver_airports",
  table_properties={"quality":"silver"},
  comment="Silver table for passengers"
)

dlt.create_auto_cdc_flow(
  target="silver_airports",
  source="airports_view",
  keys=["airport_id"],
  sequence_by=col("modified_date"),
  except_column_list=["_rescued_data"],
  stored_as_scd_type=1
)

#silver transformation for flights

@dlt.view(
  name="flights_view"
)
def flights_view():
  df=spark.readStream.format("delta").load("/Volumes/flights/bronze/bronze_volume/flights/data/")

  df_transformed=df.withColumn("flight_date",to_date(col("flight_date"),"yyyy-MM-dd"))\
                  .withColumn("modified_date",current_timestamp())
  return df_transformed

dlt.create_streaming_table(
  name="silver_flights",
  table_properties={"quality":"silver"},
  comment="Silver table for flights"
)

dlt.create_auto_cdc_flow(
  target="silver_flights",
  source="flights_view",
  keys=["flight_id"],
  sequence_by=col("modified_date"),
  except_column_list=["_rescued_data"],
  stored_as_scd_type=1
)


