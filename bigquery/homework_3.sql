-- Creating external table referring to gcs path
CREATE EXTERNAL TABLE `dataeng-376620.nytaxi.external_green_taxi_2022`
OPTIONS (
  format='PARQUET',
    uris=['https://storage.cloud.google.com/mage-zoomcamp-kristina-marunich/green_taxi_2022.parquet']
);

-- check data
select * from `dataeng-376620.nytaxi.external_green_taxi_2022` limit 100;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dataeng-376620.nytaxi.green_taxi_2022_non_partitoned AS
SELECT * FROM dataeng-376620.nytaxi.external_green_taxi_2022;

-- homework 1
select count(*) from dataeng-376620.nytaxi.green_taxi_2022_non_partitoned;

-- homework 2
select count(distinct PULocationID) from dataeng-376620.nytaxi.external_green_taxi_2022;
select count(distinct PULocationID) from dataeng-376620.nytaxi.green_taxi_2022_non_partitoned;

-- homework 3
select count(*) from dataeng-376620.nytaxi.green_taxi_2022_non_partitoned where fare_amount = 0;

-- homework 4

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE dataeng-376620.nytaxi.green_taxi_2022_partitoned_clustered
PARTITION BY DATE(cleaned_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime FROM  dataeng-376620.nytaxi.external_green_taxi_2022;

-- homework 5
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
select distinct PULocationID from dataeng-376620.nytaxi.green_taxi_2022_non_partitoned 
where date(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) >= '2022-06-01'
and date(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) <= '2022-06-30';

select distinct PULocationID from dataeng-376620.nytaxi.green_taxi_2022_partitoned_clustered
where cleaned_pickup_datetime >= '2022-06-01'
and cleaned_pickup_datetime <= '2022-06-30';