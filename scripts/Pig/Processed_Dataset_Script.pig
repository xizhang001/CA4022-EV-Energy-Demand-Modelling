-- This file contains Pig Latin code used to pre-process and clean data in order to create Processed_Dataset.
-- Dataset1_Creation_Script.py is then run on Processed_Dataset to create Dataset1.
-- Dataset2_Creation_Script.py is then run on Processed_Dataset to create Dataset2.
-- Dataset3_Creation_Script.py is then run on Processed_Dataset to create Dataset3.

------------------------------------------------------------------------------------------------------------------------------------------
-- Load EV demand dataset
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
station_data = LOAD 'raw_data/Palo_Alto_EV_Data.csv' USING CSVLoader();

------------------------------------------------------------------------------------------------------------------------------------------
-- Fix Station_Name typo and remove duplicate rows
clean_table = FOREACH station_data GENERATE (chararray) REPLACE($0,'PALO ALTO CA / BRYANT # 1','PALO ALTO CA / BRYANT #1') AS Station_Name,
 (chararray) $3 AS Start_Date, (chararray) $9 AS Charging_Time, (double) $10 AS Energy;
distinct_clean_table = DISTINCT clean_table;

------------------------------------------------------------------------------------------------------------------------------------------
-- Add station cluster names to dataset
station_clusters_data = LOAD 'raw_data/Palo_Alto_Station_Clusters_Data.csv' USING CSVLoader();
station_clusters_table = FOREACH station_clusters_data GENERATE (chararray) $0 AS Station_Cluster_Name, (chararray) $1 AS Station_Name;
joined_table = JOIN distinct_clean_table BY Station_Name, station_clusters_table BY Station_Name;

------------------------------------------------------------------------------------------------------------------------------------------
-- Fix date format
date_table = FOREACH joined_table GENERATE $0, ToDate($1, 'M/d/y H:m') AS Start_Date, $2, $3, $4;
ordered_table = ORDER date_table by Start_Date ASC;
final_table = FOREACH ordered_table GENERATE $0, ToString($1, 'd/M/y H:m') AS Start_Date, $2, $3, $4;

------------------------------------------------------------------------------------------------------------------------------------------
-- Store dataset
STORE final_table INTO 'clean_data/Processed_Dataset' USING CSVExcelStorage();
