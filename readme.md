# VRA - Videogame Recommender Algorithm
Program designed to extract and treat data from several videogame databases, with the objective of developing an algorithm able to recommend a videogame to the user based in his likes. It's based on Python 3.9 and works with AWS.

## Table of contents
* [General info] (#general-info)
* [Technologies] (#technologies)
* [Setup] (#setup)
* [Databases] (#databases)

## General info
This project works as an ETL, extracting data from 4 different online databases, IGDB, HLTB, OpenCritic and RAWG.io.
In order to do this, the program makes requests to IGDB API, from where most of the info of each game is obtained.
When this info is obtained, a scrapper enters into action in order to obtain the approximate duration of each game, from the webpage HowLongToBeat.
Another scrapper is used to obtain a critics' score from OpenCritic.
Finally, an API is used to obtaind data related to the developers of each game. This API works with RAWG.io
The results are merged into a DataFrame and is exported into a S3 Bucket

## Technologies
Project is created with:
* Python 3.9
* BeautifulSoup4 4.11.1
* Fuzzywuzzy 0.18.0
* Pandas 1.4.4
* Requests 2.28.1
* S3fs 2022.10.0
* User_agent 0.1.10

## Setup
To run this project, you'll need to install the libraries noted in requirements.txt.
This project is made to work inside AWS.
You'll need access to IGDB and RAWG APIs to have access to their data.
A file named secrets.toml containing the S3 Bucket name and credentials for the different APIs isn't uploaded.

## Databases
* IGDB: Contains info related with name, release dates, companies involved, genre, franchises...
* HLTB: Info related to a game's duration.
* OpenCritic: Critics' Score.
* RAWG: Although is similar to IGDB, due to their API restrictions, it's only used to obtain Metacritic's Rating and the devs involved in a game development.