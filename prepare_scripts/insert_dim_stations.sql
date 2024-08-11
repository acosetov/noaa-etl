-- Switch to the 'noaa' database
\c noaa

INSERT INTO dim_stations (station_id, dataset_id, city_id, name, latitude, longitude, elevation, elevation_unit, mindate, maxdate, datacoverage) VALUES ('SP000008181', 'GHCND', 'SP000001', 'BARCELONA AEROPUERTO, SP', 41.2928, 2.0697, 4, 'METERS', '1924-03-01', '2024-08-08', 0.9377);
INSERT INTO dim_stations (station_id, dataset_id, city_id, name, latitude, longitude, elevation, elevation_unit, mindate, maxdate, datacoverage) VALUES ('SP000008215', 'GHCND', 'SP000006', 'NAVACERRADA, SP', 40.7806, -4.0103, 1894, 'METERS', '1946-01-01', '2024-08-08', 1.0000);
INSERT INTO dim_stations (station_id, dataset_id, city_id, name, latitude, longitude, elevation, elevation_unit, mindate, maxdate, datacoverage) VALUES ('SPM00008284', 'GHCND', 'SP000018', 'VALENCIA, SP', 39.489, -0.482, 68.6, 'METERS', '1973-01-01', '2024-08-08', 0.9992);
