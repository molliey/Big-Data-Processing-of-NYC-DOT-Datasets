# Big Data Processing of NYC DOT Datasets

This project focuses on processing and analyzing various datasets including traffic flow, air quality, and resident condition data to identify correlations. The project implements big data processing techniques using Hadoop MapReduce and provides data visualization using Tableau.


## Data Sources

### DOT Traffic Speeds NBE
- **Source**: https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds-NBE/i4gi-tjb9/about_data
- **Description**: The dataset provides real-time New York traffic data, including speed, travel time, timestamps, boroughs, and road segment information.
- **Size**: 1M+ rows

![NYC DOT Traffic Speeds Dataset](./images/nyc_traffic_speeds.png)

*NYC OpenData portal showing DOT Traffic Speeds NBE dataset with real-time traffic data including speed, travel time, and road segment information*

### Real-Time Air Quality: PM2.5 in New York City (April 2025)
- **Source**: https://a816-dohbesp.nyc.gov/IndicatorPublic/data-features/realtime-air-quality/
- **Description**: The dataset captures hourly PM2.5 levels in New York City, and real-time PM2.5 air quality data from 14 monitoring sites.
- **Size**: 20,000 rows

![NYC PM2.5 Air Quality Dashboard](./images/nyc_pm25_dashboard.png)

*NYC PM2.5 Air Quality Dashboard showing hourly trends, geographic distribution of monitoring sites, and compliance with NAAQS 24-hour standard (35 µg/m³)*

### Displacement Risk
- **Source**: https://a816-dohbesp.nyc.gov/IndicatorPublic/data-features/displacement-risk/
- **Description**: The dataset combines multiple factors to evaluate the risk of residents being displaced from their neighborhoods. Factors include population vulnerability, housing conditions, market pressure, various socioeconomic indicators.
- **Size**: 195 rows, 31 columns

![NYC Displacement Risk Map](./images/nyc_displacement_risk.png)

*NYC Equitable Development Data Explorer showing displacement risk map with color-coded neighborhoods indicating risk levels from highest (dark purple) to lowest (light pink)*

## Data Pipeline

![Data Processing and Analysis Workflow](./images/data_workflow.png)

*Data Processing and Analysis Workflow showing the five-step pipeline from geographic mapping to insights generation*

### 1. Data Preprocessing and Location Matching

- Loads and processes traffic data (`traffic_202411_update.csv`)
- Matches traffic links with monitoring sites using spatial analysis
- Aggregates traffic data by time intervals
- Cleans and formats final joined datasets

### 2.1 Traffic Data Processing

- **trafficFlow**: Main MapReduce job for traffic flow analysis
- **trafficFlowMapper**: Processes individual traffic records
- **trafficFlowReducer**: Aggregates traffic flow statistics
- **Technologies**: Hadoop MapReduce (Java)

### 2.2 Air Quality Data Processing

- **DataCleaning**: Cleans air quality data and replaces site IDs with site names
- **DataProfiling**: Performs comprehensive data profiling and analysis
- **Technologies**: Hadoop MapReduce (Java)

### 2.3 Resident Condition Data Processing

- **CategoryMapping**: Maps and categorizes resident condition data
- **convert_to_csv.py**: Converts MapReduce output to CSV format
- **Technologies**: Hadoop MapReduce (Java), Python

### 3. Data Analysis and Visualization

- **report_tableau.twb**: Tableau workbook with comprehensive data visualizations

### NYC DOT Datasets

**Input:** Cleaned traffic data with the following fields:
- **SPEED**: The average speed of vehicles on a road segment (in miles per hour).
- **TRAVEL_TIME**: The average travel time for the segment (in seconds).
- **DATA_AS_OF**: The timestamp for the data record.
- **BOROUGH**: The borough in New York City (e.g., Manhattan, Bronx).
- **LINK_NAME**: A description of the road segment.

![Traffic Data Input Example](./images/traffic_input.png)
*Example of the cleaned NYC DOT Traffic Speeds dataset input.*

**Processing:**
Utilized Hadoop MapReduce to processe the input data to calculate relative hourly average traffic flow for each road segment and borough.

**Formula:**
```
Traffic Flow = SPEED / TRAVEL_TIME (relative)
```

**Output:** Aggregated hourly traffic flow statistics for each BOROUGH and LINK_NAME.

![Traffic Data Output Example](./images/traffic_output.png)
*Example of the processed traffic flow output, showing aggregated hourly statistics.*

## Data Analysis

![NYC Data Analysis Results](./images/analysis_results.png)

*Results slide showing correlations among air quality, traffic patterns, and displacement risk in NYC with key findings and AQI trends*



