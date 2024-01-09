import sys
from datetime import datetime

from sodapy import Socrata
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType
from pyspark.sql.functions import col, substring


schema = StructType([
    StructField('incident_number', StringType(), True),
    StructField('exposure_number', StringType(), True),
    StructField('id', StringType(), True),
    StructField('address', StringType(), True),
    StructField('incident_date', StringType(), True),
    StructField('call_number', StringType(), True),
    StructField('alarm_dttm', StringType(), True),
    StructField('arrival_dttm', StringType(), True),
    StructField('close_dttm', StringType(), True),
    StructField('city', StringType(), True),
    StructField('zipcode', StringType(), True),
    StructField('battalion', StringType(), True),
    StructField('station_area', StringType(), True),
    StructField('box', StringType(), True),
    StructField('suppression_units', StringType(), True),
    StructField('suppression_personnel', StringType(), True),
    StructField('ems_units', StringType(), True),
    StructField('ems_personnel', StringType(), True),
    StructField('other_units', StringType(), True),
    StructField('other_personnel', StringType(), True),
    StructField('first_unit_on_scene', StringType(), True),
    StructField('fire_fatalities', StringType(), True),
    StructField('fire_injuries', StringType(), True),
    StructField('civilian_fatalities', StringType(), True),
    StructField('civilian_injuries', StringType(), True),
    StructField('number_of_alarms', StringType(), True),
    StructField('primary_situation', StringType(), True),
    StructField('mutual_aid', StringType(), True),
    StructField('action_taken_primary', StringType(), True),
    StructField('action_taken_secondary', StringType(), True),
    StructField('action_taken_other', StringType(), True),
    StructField('detector_alerted_occupants', StringType(), True),
    StructField('property_use', StringType(), True),
    StructField('area_of_fire_origin', StringType(), True),
    StructField('ignition_cause', StringType(), True),
    StructField('ignition_factor_primary', StringType(), True),
    StructField('ignition_factor_secondary', StringType(), True),
    StructField('heat_source', StringType(), True),
    StructField('item_first_ignited', StringType(), True),
    StructField('human_factors_associated_with_ignition', StringType(), True),
    StructField('structure_type', StringType(), True),
    StructField('structure_status', StringType(), True),
    StructField('floor_of_fire_origin', StringType(), True),
    StructField('fire_spread', StringType(), True),
    StructField('no_flame_spead', StringType(), True),
    StructField('number_of_floors_with_minimum_damage', StringType(), True),
    StructField('number_of_floors_with_significant_damage', StringType(), True),
    StructField('number_of_floors_with_heavy_damage', StringType(), True),
    StructField('number_of_floors_with_extreme_damage', StringType(), True),
    StructField('detectors_present', StringType(), True),
    StructField('detector_type', StringType(), True),
    StructField('detector_operation', StringType(), True),
    StructField('detector_effectiveness', StringType(), True),
    StructField('detector_failure_reason', StringType(), True),
    StructField('automatic_extinguishing_system_present', StringType(), True),
    StructField('automatic_extinguishing_system_type', StringType(), True),
    StructField('automatic_extinguishing_system_performance', StringType(), True),
    StructField('automatic_extinguishing_system_failure_reason', StringType(), True),
    StructField('number_of_sprinkler_heads_operating', StringType(), True),
    StructField('supervisor_district', StringType(), True),
    StructField('neighborhood_district', StringType(), True),
    StructField('point', StructType([
        StructField('type', StringType(), True),
        StructField('coordinates', ArrayType(StringType()), True)
    ]), True)
])

if __name__ == '__main__':        
    
    # get API data for an specific day
    if len(sys.argv) == 1:
        execution_date = None
    else:
        execution_date = datetime.strptime(sys.argv[1],'%Y-%m-%d')

    client = Socrata("data.sfgov.org", None)
    spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())
    spark.sparkContext.setLogLevel('WARN') # disable info logger because on historical processing take a long time
    
    offset=-1
    limit=50000
    df_final = spark.createDataFrame([], schema=schema) # creates empty pyspark dataframe to be filled with API result data

    ## GET RESULTS FROM API ##
    # while True for pagination
    while True:
        print(f"###### LIMIT = {limit} OFFSET = {offset+1} TOTAL_ROWS = {df_final.count()} ######")

        # Get results from API and store on a dataframe
        if execution_date is not None:
            results = client.get("wr8u-xric", where=f"incident_date like '{execution_date.strftime('%Y-%m-%d')}%'", offset=offset+1, limit=limit)
        else:
            # full processing
            results = client.get("wr8u-xric", offset=offset+1, limit=limit)
        df = spark.createDataFrame(results, schema=schema)

        # Union data with final dataframe due to pagination
        df_final = df_final.unionAll(df)

        # pagination logic
        if len(results) != 0:
            offset += limit
        else:
            break

    ## DATAFRAME TYPE CASTING ##
    df_final = df_final.withColumn('incident_number', col('incident_number').cast('int'))
    df_final = df_final.withColumn('exposure_number', col('exposure_number').cast('int'))
    df_final = df_final.withColumn('suppression_units', col('suppression_units').cast('int'))
    df_final = df_final.withColumn('suppression_personnel', col('suppression_personnel').cast('int'))
    df_final = df_final.withColumn('ems_units', col('ems_units').cast('int'))
    df_final = df_final.withColumn('ems_personnel', col('ems_personnel').cast('int'))
    df_final = df_final.withColumn('other_units', col('other_units').cast('int'))
    df_final = df_final.withColumn('other_personnel', col('other_personnel').cast('int'))
    df_final = df_final.withColumn('fire_fatalities', col('fire_fatalities').cast('int'))
    df_final = df_final.withColumn('fire_injuries', col('fire_injuries').cast('int'))
    df_final = df_final.withColumn('civilian_fatalities', col('civilian_fatalities').cast('int'))
    df_final = df_final.withColumn('civilian_injuries', col('civilian_injuries').cast('int'))
    df_final = df_final.withColumn('number_of_alarms', col('number_of_alarms').cast('int'))
    df_final = df_final.withColumn('floor_of_fire_origin', col('floor_of_fire_origin').cast('int'))
    df_final = df_final.withColumn('number_of_floors_with_minimum_damage', col('number_of_floors_with_minimum_damage').cast('int'))
    df_final = df_final.withColumn('number_of_floors_with_significant_damage', col('number_of_floors_with_significant_damage').cast('int'))
    df_final = df_final.withColumn('number_of_floors_with_heavy_damage', col('number_of_floors_with_heavy_damage').cast('int'))
    df_final = df_final.withColumn('number_of_floors_with_extreme_damage', col('number_of_floors_with_extreme_damage').cast('int'))
    df_final = df_final.withColumn('number_of_sprinkler_heads_operating', col('number_of_sprinkler_heads_operating').cast('int'))

    # Define partition columns
    partition_cols = ["incident_date", "neighborhood_district", "battalion"]

    # Write output to Parquet files in MinIO
    if execution_date is not None:
        # processing single day
        print(f"!!!!!!!!!!!!!!!!!!!!! Saving parquet file for date {execution_date}")
        output_path = f"s3a://de-challenge/fire_incidents/{execution_date.strftime('%Y-%m-%d')}"
        df_final.write.mode("overwrite").partitionBy(partition_cols).parquet(output_path)
    else:
        print("#")
        print("#")
        print("#")
        # processing full history
        distinct_dates = df_final.select(substring(col('incident_date'), 1, 10).alias('date')).distinct().rdd.flatMap(lambda x: x).collect()
        print(distinct_dates)
        for date in distinct_dates:
            # Filter the DataFrame for the current date
            print(f"!!!!!!!!!!!!!!!!!!!!! Saving parquet file for date {date}")
            filtered_df = df_final.filter(substring(col('incident_date'), 1, 10) == date)
            output_path = f"s3a://de-challenge/fire_incidents/{date}"
            filtered_df.write.mode("overwrite").partitionBy(partition_cols).parquet(output_path)

    # Stop SparkSession
    spark.stop()