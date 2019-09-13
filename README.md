# AQMap: Finding Safe Air

## Introduction
Finding current air quality is easy, but what is the air 
quality in your neighborhood like over the course of the 
year? Which pollutants contribute most? Are you close to 
freeways where cars release pollutants that aren't even 
tracked? Find out with AQMap. 

## Data Sets
The EPA air quality dataset from US monitors 1980-present
will be used, along with highway data from OpenStreetMap. 

## Engineering Challenges
This project requires joining two unrelated data sources
and performing fast batch computations on ~250 GB of data.

## Business Value
City planners or developers can use the data to choose healthy
locations for development, and individuals can pick accomodations 
in healthier areas. 

## Presentation 

The user can select a location and a five-year date range. 
Line plots show pollutant concentrations and air quality over
the course of the year, averaged over the five-year period. 
Points on a map mark freeways near the selected location. 
The mean numbers of days where pollutants exceeded the EPA maximum 
concentrations are highlighted in boxes. 
