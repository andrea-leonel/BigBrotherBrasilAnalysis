# Overview

This project scrapes data from Wikipedia about Brazil's most watched show, Big Brother Brasil. This data is then ingested into a GCP bucket and turn into BigQuery datasets. Normalisation and facts table creation happens in dbt before insights are gathered and visualised using Power BI.

## Table of Contents
- [Problem Description](#problem-description)
- [Workflow & Technologies](#workflow--technologies)
- [The Dataset](#the-dataset)
- [Future Improvements](#future-improvements)
- [Reproducibility](#reproducibility)

# Problem Description

<img src="https://variety.com/wp-content/uploads/2021/03/big-brother-brasil.jpg?w=1000&h=563&crop=1" width="500" height="300"/>

In its 25 years of existence, the Big Brother franchise has become a cultural phenomenon in Brazil. With a bespoke format to appeal to the Brazilian audience, the show sparks debates on topics like diversity, ethics, and relationships on social platforms. This engagement isn't just limited to fans; brands also leverage the buzz to connect with audiences, with its 2024 edition raising around 175 million dollars (1 billion Brazilian reais) in advertising revenue. Its contestants go on to become celebrities in the country with many going on to build a solid television career.

Each year, the show generates a large volume of information on contestants, nominations, ratings, evictions and more. Currently, there is no centralised database with all this information properly stored and normalised for analysis. Therefore, the scope of this project is to fill that gap and create a structure for the collection of data for future seasons to allow for fans and journalists to gather key insights on the show and its history.

# Workflow & Technologies

[![Workflow.png](https://i.postimg.cc/yY4Sc245/Workflow.png)](https://postimg.cc/5jg0M7tq)

1) Webscraping using Python: raw data is collected from Wikipedia pages and turned into csv tables.
2) Ingestion & Storage using Kestra, GCP, BigQuery: these csv tables are stored into BigQuery datasets on GCP using dynamic Kestra flows.
3) Nomalisation & Manipulation using dbt: individual shows tables are consolidated into a normalised dataset using dbt along with the creation of a facts table for analysis.
4) Visualisation: Visuals displaying key insights on the show are created using Power BI.

# The Dataset

*Please note the dataset is partially in Brazilian Portuguese as it is intended for users in Brazil. Table names and code were kept in English for this project*  
For clarity:  
Edicao = Season  
Participante = Contestant  
Semana = Week  
Paredao = Nomination


The dataset is comprised of 6 tables:  
**1) dm_contestants**: demographic and show performance informationn per contestant. Keys: id_participante and edicao.  
**2) dm_nominations**: information on the nomination rounds such as who was the head-of-house, who had power of immunity, who was evicted, etc. This table has a long format (unpivotted) with an event attribute and id_participante as values. Keys: edicao+semana, edicao and id_participante.  
**3) dm_noms_desc**: links season, week and nomination_rounds, necessary for weekly analysis across all the tables. Keys: edicao, edicao+semana  
**4) dm_ratings**: each season's ratings in weekly average and by day (available only for seasons 10-12 and 15 onwards). Keys: edicao+semana  
**5) fact_edicao**: key stats per season. Keys: edicao.  
**6) fact_contestant**: key stats per contestant. Keys: id_participante.

# Future Improvements

1) There are certain inconsistencies in the dm_nominations table that are a result of the unstructured nature of the Wikipedia tables. This information would need to be gathered from the notes on Wikipedia and manually adjusted in the dataset. For example, immunities given outside the Power of Immunity are not reflected in the dataset. Also, some nominations are not captured correctly; for example, when the second most-voted is also nominated, that should be a separate event.
2) There is a mistake on dm_contestants with some Brazilian states being considered foreign. The webscraping script needs to be checked and fixed.
3) This dataset lacks information on a very important dimension of this show's popularity: social media. It would be interesting to see the follower growth of the contestants throughout the show and how they maintained that growth post-show. It would also be interesting to gather mention information, particularly on X, where the show is heavily commented on by its fans.
4) Besides, information on the challenges could be added, something that [BBBStats](https://drive.google.com/drive/u/0/folders/1O9LwFF4oR-n3SNd1vY_v-7n8QhDeprRv) started doing and that could be put in table form and joined with the rest of this database.
5) There are other tables with consistent information across the seasons on their Wikipedia pages that could be further explored too.

# Reproducibility

### 1) Download WSL:
Kestra runs on a Docker image and Docker runs more smoothly on Linux. So, if you're using Windows, [download WSL](https://learn.microsoft.com/en-us/windows/wsl/install) to create a virtual Linux machine.

### 2) Setting up GCP: creating a new project and setting up service accounts
- Create a new project on Google Cloud Console
- These are the service accounts necessary under IAM for this project to work:
  - For Terraform: BigQuery Admin, Compute Admin, Storage Admin
  - For Kestra: BigQuery Admin and Storage Admin
  - For dbt: BigQuery Admin, BigQuery Data Editor, BigQuery Job User, BigQuery User
 
### [3) Create a GCP bucket using Terraform](1_terraform/)

### [4) Create the flows on Kestra](2_kestra/)

### [5) Running the models on dbt](4_dbt/)

### [6) Overview Dashboard on PowerBI](5_powerbi/)




