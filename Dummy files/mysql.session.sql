
use mysourcedb;

create table corona_pandemic_table(
province_state  VARCHAR(45),
country_region VARCHAR(45),
date DATE,
latitude FLOAT,
longitude FLOAT,
sub_region1_name VARCHAR(45),
location_geom VARCHAR(45),
confirmed INTEGER,
deaths INTEGER,
recovered INTEGER,
active INTEGER);

LOAD DATA INFILE 'gs://dataproc-gs/sql-data/1.csv'
 INTO TABLE corona_pandemic_table
 FIELDS TERMINATED BY ','
 ENCLOSED BY '"'
 LINES TERMINATED BY '\n'
 IGNORE 1 LINES;

-- HIVE

CREATE TABLE IF NOT EXISTS corona_pandemic_table(
    province_state  STRING,
country_region STRING,
latitude FLOAT,
longitude FLOAT,
sub_region1_name STRING,
location_geom STRING,
confirmed INT,
deaths INT,
recovered INT,
active INT)
COMMENT 'A table to store corona data from sqoop job'
PARTITIONED BY (date DATE)
STORED AS PARQUET;

-- Hive Stage
CREATE TABLE IF NOT EXISTS mytestdb.corona_pandemic_table_stage(
    province_state  STRING,
country_region STRING,
record_date DATE,
latitude FLOAT,
longitude FLOAT,
sub_region1_name STRING,
location_geom STRING,
confirmed INT,
deaths INT,
recovered INT,
active INT)
COMMENT 'A table to store corona data from sqoop job'
STORED AS PARQUET;


-- SQOOP

 sqoop import --connect jdbc:mysql://9.43.61.98/test --username=root --password=root --table=corona_pandemic_table --hive-home=/user/hive/warehouse  --create-hive-table --hive-import  --hive-table=mytestdb.corona_table_stage --split-by=date

 sqoop import --connect jdbc:mysql://9.43.61.98/test --username=root --password=root --table=corona_pandemic_table --hive-import  --hive-table=mytestdb.corona_table_stage


INSERT OVERWRITE TABLE mytestdb.corona_pandemic_table PARTITION (date) SELECT * FROM mytestdb.corona_pandemic_table_stage ;