CREATE KEYSPACE IF NOT EXISTS station WITH replication =
  {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS station.stations (
  city text,
  station_id text,
  bikes smallint,
  capacity smallint,
  updated_at timestamp,
  PRIMARY KEY ((city, station_id), updated_at) );

-- CREATE TYPE station.address (
--   street text,
--   city text,
--   state_or_province text,
--   postal_code text,
--   country text );

-- CREATE TABLE station.stations_by_poi (
--   poi_name text,
--   station_id text,
--   name text,
--   phone text,
--   address frozen<address>,
--   PRIMARY KEY ((poi_name), station_id) )
--   WITH comment = ‘Q1. Find stations near given poi’
--   AND CLUSTERING ORDER BY (station_id ASC) ;

-- CREATE TABLE station.stations (
--   id text PRIMARY KEY,
--   name text,
--   phone text,
--   address frozen<address>,
--   pois set )
--   WITH comment = ‘Q2. Find information about a station’;

-- CREATE TABLE station.pois_by_station (
--   poi_name text,
--   station_id text,
--   description text,
--   PRIMARY KEY ((station_id), poi_name) )
--   WITH comment = Q3. Find pois near a station’;

-- CREATE TABLE station.available_rooms_by_station_date (
--   station_id text,
--   date date,
--   room_number smallint,
--   is_available boolean,
--   PRIMARY KEY ((station_id), date, room_number) )
--   WITH comment = ‘Q4. Find available rooms by station date’;

-- CREATE TABLE station.amenities_by_room (
--   station_id text,
--   room_number smallint,
--   amenity_name text,
--   description text,
--   PRIMARY KEY ((station_id, room_number), amenity_name) )
--   WITH comment = ‘Q5. Find amenities for a room’;
