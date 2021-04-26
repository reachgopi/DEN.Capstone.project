CREATE TABLE IF NOT EXISTS immig_fact (
cicid INTEGER NOT NULL,
i94_port VARCHAR distkey,
i94_citizen INTEGER,
i94_resident INTEGER,
i94_arrival_date INTEGER,
travel_mode_id INTEGER,
state_code VARCHAR,
i94_departure_date INTEGER,
i94_age INTEGER,
visa_category_id INTEGER,
i94_date_added DATE,
i94_date_admitted DATE,
i94_occupation VARCHAR,
i94_arrival_flag VARCHAR,
i94_departure_flag VARCHAR,
i94_update_flag VARCHAR,
i94_match_flag VARCHAR,
i94_birth_year INTEGER,
i94_gender VARCHAR,
i94_insnum VARCHAR,
i94_airline VARCHAR,
i94_admission_number DOUBLE PRECISION,
i94_flight_number VARCHAR,
i94_visa_type VARCHAR,
PRIMARY KEY(cicid)) 

CREATE TABLE IF NOT EXISTS visa_type_dim (
visa_category_id INTEGER NOT NULL,
visa_type VARCHAR,
PRIMARY KEY(visa_category_id)) 
diststyle all

CREATE TABLE IF NOT EXISTS travel_mode_dim (
travel_mode_id INTEGER NOT NULL,
travel_mode VARCHAR,
PRIMARY KEY(travel_mode_id)) 
diststyle all

CREATE TABLE IF NOT EXISTS time_dim (
arrival_sas INTEGER NOT NULL,
arrival_date DATE,
day INTEGER,
month INTEGER,
year INTEGER,
weekday varchar,
PRIMARY KEY(arrival_sas))
diststyle all

CREATE TABLE IF NOT EXISTS airport_dim (
airport_code varchar NOT NULL distkey,
iata_code varchar,
airport_name varchar,
airport_type varchar,
region varchar,
country_code varchar,
municipality varchar,
gps_code varchar, 
coordinates varchar,
PRIMARY KEY(airport_code))

CREATE TABLE IF NOT EXISTS demographics_dim (
stateCode varchar NOT NULL,
average_median_age DOUBLE PRECISION,
average_male_population DOUBLE PRECISION,
average_female_population DOUBLE PRECISION,
average_total_population DOUBLE PRECISION,
average_no_of_veterans DOUBLE PRECISION,
average_foreign_born DOUBLE PRECISION,
average_house_hold_size DOUBLE PRECISION, 
PRIMARY KEY(stateCode))
diststyle all

CREATE TABLE IF NOT EXISTS country_temperature_dim (
country_code INTEGER NOT NULL,
country varchar,
avg_country_temperature DOUBLE PRECISION,
avg_country_temperature_uncertainty DOUBLE PRECISION,
PRIMARY KEY(country_code))
diststyle all