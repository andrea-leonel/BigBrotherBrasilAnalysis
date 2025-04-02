# Overview

This project scrapes data from Wikipedia about Brazil's most watched show, Big Brother Brasil. This data is then ingested into a GCP bucket and turn into BigQuery datasets. Normalisation and facts table creation happens in dbt before insights are gathered and visualised using Power BI.

# Problem Description

<img src="https://variety.com/wp-content/uploads/2021/03/big-brother-brasil.jpg?w=1000&h=563&crop=1" width="500" height="300"/>

In its 25 years of existence, the Big Brother franchise has become a cultural phenomenon in Brazil. With a bespoke format to appeal to the Brazilian audience, the show sparks debates on topics like diversity, ethics, and relationships on social platforms. This engagement isn't just limited to fans; brands also leverage the buzz to connect with audiences, with its 2024 edition raising around 175 million dollars (1 billion Brazilian reais) in advertising revenue. Its contestants go on to become celebrities in the country with many going on to build a solid television career.

Each year, the show generates a large volume of information on contestants, nominations, ratings, evictions and more. Currently, there is no centralised database with all this information properly stored and normalised for analysis. Therefore, the scope of this project is to fill that gap and create a structure for the collection of data for future editions in order to allow for fans and journalists to gather key insights on the show and its history.

# Workflow & Technologies

![Workflow of the project](https://i.ibb.co/C3DCZfJx/Workflow.png)

1) Webscraping using Python: raw data is collected from Wikipedia pages and turned into csv tables.
2) Ingestion & Storage using Kestra, GCP, BigQuery: these csv tables are stored into BigQuery datasets on GCP using dynamic Kestra flows.
3) Nomalisation & Manipulation using dbt: individual shows tables are consolidated into a normalised dataset using dbt along with the creation of a facts table for analysis.
4) Visualisation: Visuals displayig key insights on the show are created using Power BI.

# The Data

The data is comprised of 3 tables:
1) Contestants: demographic information of each contestant as well as their position in the show and other show-related information (eg method of entry).
2) Nominations: for each eviction round, information on the nomination of the head-of-house, the nomination of the house, and other types of nominations.
3) Eviction Results: for each eviction round, information on the % of public votes for each contestant and who was evicted.
4) Shows: information on the format of each show, the host, number of episodes and, for some years, ratings.

# Future Improvements

This dataset lacks information on a very important dimension of this show's popularity: social media. It would be interesting to see follower growth of the contestants throughout the show and how they maintained that growth post-show. It would also be interesting to gather mention information, particularly on X where the show is heavily commented by its fans.

Besides, information on the challenges could be added, something that [BBBStats](https://drive.google.com/drive/u/0/folders/1O9LwFF4oR-n3SNd1vY_v-7n8QhDeprRv) started doing and that could be put in table form and joined with the rest of this database.

# Reproducibility
TBD
