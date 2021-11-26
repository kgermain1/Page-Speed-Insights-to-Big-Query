# Page Speed Insights to Big Query
Script to automatically pull Google Page Speed Insights Data into Google Big Query for multiple URLs. Uses threading to reduce execution time.

Designed to be run in Google Cloud Functions and tested for up to 50 URLs.

Also includes 2 separate files for classes: 1 to pull data from the Google Page Spee Insights APi, and 1 to load a dataframe into BigQuery.
