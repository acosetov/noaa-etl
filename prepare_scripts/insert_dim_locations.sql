-- Switch to the 'noaa' database
\c noaa

INSERT INTO dim_locations (city_id, dataset_id, mindate, maxdate, name, datacoverage) VALUES ('SP000001', 'GHCND', '1913-08-06', '2024-08-08', 'Barcelona, SP', 1.0000);
INSERT INTO dim_locations (city_id, dataset_id, mindate, maxdate, name, datacoverage) VALUES ('SP000006', 'GHCND', '1920-01-01', '2024-08-08', 'Madrid, SP', 1.0000);
INSERT INTO dim_locations (city_id, dataset_id, mindate, maxdate, name, datacoverage) VALUES ('SP000018', 'GHCND', '1973-01-01', '2024-08-08', 'Valencia, SP', 0.9992);
