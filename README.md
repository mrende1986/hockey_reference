# hockey_reference
![Alt text here](/img/hockey_reference.drawio.svg)
## Overview
When I watch hockey I love when the announcers give obscure player or team stats (i.e. the team is 10-0-1 when scoring first). The problem is when you go to NHL.com they don't have granualar team or player data available. This project is designed to setup a data warehouse on your local machine that you can use to find your own obscure stats.

## Techincal Overview
The pipeline scrapes data from hockeyreference.com and Google Sheets. Next the data is cleaned so it can be used for analysis and loaded into the Postgres datawarehouse. Airflow is used for orchestration and hosted locally with docker-compose and mysql. Postgres is also running locally in a docker container. The data dashboard is on Tableau Public.
