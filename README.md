Apache Airflow ETL Project
========

Welcome to my Etl project the main goal of this code is to exctract top anime data from an api, transform it into readable data and extract it to postgres as a readable database, this project should run automatically weekly to ensure constant up to date rankings. you can find the Pipeline diagram in the repo it is called (etldiagram)

Requirements to run 
================
Python  
Docker  
Apache Airflow  
Astronomer  
DBeaver (optional to see database)  

How to Set Up and Run the Project 
===========================
1. make sure you download docker desktop downloaded
2. clone this repo onto your computer
3. open the project in VSCode or similar
4. In the terminal run the command (pip install "apache-airflow[celery]==3.02") and (winget install -e --id Astronomer Astro)
5. in the terminal run (astro dev init) and then (astro dev start)
6. open Docker and run the DAG this should now run weekly
to see the project running go to http://localhost:8080
if you have DBeaver installed you can then connect to the database in postgress and view the table that has been created 

Contribution Guidelines
=======
This project takes the top 50 Anime from animenews api and puts it into a easy to read database, to contribute to this project you can add columns from the api and fromat them how you would like this could be nice to see what other information we could give on the anime, each anime on the page has its own url which means you could go through those pages and find useful information to add, such as a brief description.
To make changes simply clone the repository and create a new branch, following this add what you need to the ETL code and give it a test run, if all of it works Great, push it up to your branch and create a merge request.

Important URLS
=======
https://www.animenewsnetwork.com/encyclopedia/reports.xml?id=172  
https://www.animenewsnetwork.com/encyclopedia/reports.php?id=172

