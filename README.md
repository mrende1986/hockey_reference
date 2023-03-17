# hockey_reference
![Alt text here](/img/hockey_reference.drawio.svg)


## Overview
The pipeline scrapes data from the hockeyreference.com and cleans data for use. Then the cleaned data is ingested into the Postgres datawarehouse. A temp table is created and then the unique rows are inserted into the data tables. Airflow is used for orchestration and hosted locally with docker-compose and mysql. Postgres is also running locally in a docker container. The data dashboard is on Tableau Public.
