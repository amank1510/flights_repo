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
  properties={"quality":"silver"},
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

  return df

dlt.create_streaming_table(name="silver_passengers",
                           properties={"quality":"silver"},
                           comment="Silver table for passengers"
                           )

dlt.create_auto_cdc_flow(
  target="silver_passengers",
  source="passengers_view",
  key=["passenger_id"],
  sequence_by=["modified_date"],
  except_column_list=["_recued_date"],
  stored_as_scd_type=1
)

#silver


