-- Switch to the 'noaa' database
\c noaa
INSERT INTO dim_datasets (dataset_id, description) VALUES ('GHCND', 'Daily Summaries');