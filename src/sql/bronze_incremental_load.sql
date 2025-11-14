CREATE OR REPLACE PROCEDURE bronze.incremental_load()
LANGUAGE plpgsql
AS $$
DECLARE
  m_lld timestamp;   -- metadata last load date
  m_last_in timestamp;      -- bronze last pickup date
  max_pickup timestamp;   -- max pickup in the incoming file
  month_start timestamp;
  month_end timestamp;
  upload_status meta.status_enum;
  start_time timestamp;
  end_time timestamp;
  execution_duration interval;
  error_message text;
BEGIN

  start_time := clock_timestamp();
  SELECT COALESCE(MAX(last_load_date), TIMESTAMP '2024-01-01 00:00:00')
  INTO m_lld
  FROM meta.metadata_table;

  SELECT DATE_TRUNC('month', m_lld) INTO month_start;

  SELECT month_start + INTERVAL '1 month' INTO month_end;
  

--   SELECT CASE WHEN v_rows > 0 THEN 'success' ELSE 'skipped' END AS status INTO upload_status;

  -- 4) incremental insert into Bronze
  INSERT INTO bronze.yellow_taxi_raw (
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
  )
  SELECT
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
  FROM raw_stage
  WHERE tpep_pickup_datetime > m_lld
    AND (tpep_pickup_datetime >= month_start AND tpep_pickup_datetime < month_end);

  -- out of range records into meta.invalid_records
  INSERT INTO meta.invalid_records (
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
  )
  SELECT
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
  FROM raw_stage
  WHERE (tpep_pickup_datetime > m_lld
              AND NOT (tpep_pickup_datetime >= month_start AND tpep_pickup_datetime < month_end)
              )
  ON CONFLICT(vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
				trip_distance, pulocationid, dolocationid, total_amount) DO NOTHING;

  SELECT MAX(tpep_pickup_datetime) INTO m_lld FROM bronze.yellow_taxi_raw
  WHERE (tpep_pickup_datetime >= month_start AND tpep_pickup_datetime < month_end);

  end_time := clock_timestamp();
  execution_duration := end_time - start_time;
  -- 5) log success in metadata
  INSERT INTO meta.metadata_table(last_load_date, status, runtime, error_message)
  VALUES (m_lld, 'success', execution_duration, NULL); --error message is NULL for success status

EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS error_message := MESSAGE_TEXT;
  end_time := clock_timestamp();
  execution_duration := end_time - start_time;
  INSERT INTO meta.metadata_table(last_load_date, status, runtime, error_message)
  VALUES (m_lld, 'failed', execution_duration, error_message);
  RAISE;
END;
$$;