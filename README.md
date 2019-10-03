# AQMap: Exploring Your Atmosphere

## Introduction
Pollution causes tens of thousands of deaths in the United States
each year. If you're choosing a new place to live, air quality 
should be on your list of considerations. Finding the air quality 
for today is easy, but what is it like year-round? AQMap provides 
an online dashboard to explore the best EPA air quality monitoring 
data available for any address in the US. 

## Data Sets
The [EPA Air Quality dataset](https://www.epa.gov/outdoor-air-quality-data) for 
three important pollutants has been used, along with complementary temperature 
information from the [NOAA Global Historical Climatology Network](https://docs.opendata.aws/noaa-ghcn-pds/readme.html).

## Data Pipeline 

![Tech Stack](https://github.com/krueg22r/aqmap/blob/master/tech_stack.png)

### Raw Data Storage
Both data sets were available for download in csv format. Yearly summary files 
for each pollutant are approximately 100 MB in size, and yearly weather data
files are approximately 1.2 GB in size. Data were initially stored in AWS S3 
buckets. 

### Data Cleaning and Processing 
Files were initially loaded from S3 using Spark. SparkSQL commands were used to 
select the desired columns, remove data that was flagged as poor-quality, 
perform monthly and averaging, and join
NOAA station data to the main observation table based on station ID. 

### Geographical Queries 
Processed data was stored in a PostGIS database, allowing selection of geographic
nearest neighbors using the efficient k-nearest-neighbors algorithm. Entries were
indexed based on their longitude and latitude, stored in a geography column, 
leading to a 10x speedup for queries grouped by location. 

### Presentation
The Postage's database can be interactively queried from the online dashboard, which 
was built using Dash. 

