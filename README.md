# PFFdb_ETL
ETL jobs for my Predictive Fantasy Football Database

These custom ETL jobs, which I wrote in Java, are what I'm using to load data into my Predictive Fantasy Football DB. Most of the data was pulled from JSON/Excel files that I got using [this](https://github.com/BurntSushi/nflgame) wonderful python API from BurntSushi.

BurntSushi also has an NFLDB project, but I decided to make my own database instead, so I could make all the design decisions. I also wanted my db to include team's home city/stadium data, as this could show potential home-field advantage, and changes when a team moves to a new city. I also wanted my data to include coaching info for a team or individual player (right now, I am just tracking Head Coach, Offensive Coordinator, and Defensive Coordinator data). I wanted to be able to write custom queries that take into account players' coaches, home-field advantage, as well as opponent information, like how a particular player performs when playing *against* a particular coach, or in an away game at a particular city/stadium.

I created the database using PostgreSQL, and I got the raw data using BurnSushi's API, as well as pulling info from Wikipedia and other public websites using JSoup HTML parser library. I've included some of my Python scripts to query the JSON data here. Most of the ETL processes, however, are Java programs using the JDBC API and SQL scripts to extract raw data, transform them to fit my db schema, and load them into the appropriate tables.
