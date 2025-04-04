1. Save the docker-compose file in a folder.
2. Access this folder on cmd and run docker compose up (the file also starts other services like pgadmin and postgres).
3. Access Kestra on [http://localhost:8080/](http://localhost:8080/)

On Kestra:
1. Create the flow [1_bbb_gcp_kv](2_kestra/1_bbb_gcp_kv.yaml) to set up all the necessary key values for Dataset name, Project ID, Bucket Name and Project Location.
2. Under the newly created Namespace, add a key value named "BBB_GCP_CREDS" and enter the GCP credentials for the service account created for Kestra in step 2.
5. On Kestra, run the following workflows to ingest the necessary data into our bucket and into a BigQuery dataset:
    - [2_bbb_gcp_ingest_contestants.yaml](2_bbb_gcp_ingest_contestants.yaml): ingestion of the Contestants table
    - [3_bbb_gcp_setup_nominations.yaml](3_bbb_gcp_setup_nominations.yaml): ingestion of the Nominations, Individual Nomations and Eviction Results table with 3 tables per season (25 season).
    - [4_bbb_gcp_setup_rankings.yaml](4_bbb_gcp_setup_rankings.yaml): ingestion of Ranking by contestant for all seasons.
    - [5_bbb_gcp_setup_ratings.yaml](5_bbb_gcp_setup_ratings.yaml): ingestrion of Ratings table with overall average rating by season.
    - [6_bbb_gcp_setup_ratingweekly.yaml](6_bbb_gcp_setup_ratingweekly.yaml): ingestion of Ratingsweekly tables by season.
