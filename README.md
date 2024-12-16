# CSCI 422 Project - Peyton Schultz

## Dwelling Data

Dwelling Data is a project with an analysis based approach that focuses on the housing market over time. The goal of this project is to analyze the housing market and identify trends and other factors.

## Ingestion
### Data Sources
The data analyzed in this project currently all comes from Zillow datasets. Specifically, these sources are the Zillow House Value Index (ZHVI) and Market Heat datasets.  
### How?
The data gets webscraped from Zillow's site on the 13th of each month. It gets scraped by a file within an Azure Databricks Service. This file then ensures that the scraped data is clean and gets stored in a Microsoft Azure Storage Account.

## Transformation
### Transformations to make
The biggest issue with these datasets is that the format is considered wide. This means that there is a column that tracks each month and year in the dataset since the year 2000. The main transformation done was turing this wide format into a longer format. The other targeted change was the creation of a delta column which allowed for a difference between one month and the month before it.
### How?
This was also done through Databricks. Using the melt function in pyspark, the transformation from wide to long was very simple. From this point the data begain to rack a year and month column, rather than several columns. The delta columns were also very easy to implement. From this, a joined table was created which would allow for some comparisons between the two datasets. The transformed data was stored in a separate directory in the same Azure Storage Account.

## Serving
### Approach
The analysis is done in Microsoft Power BI which allows for a direct connection to an Azure Account (given you have the key). The software allows many visualization formats, and the views created are listed below.
<ul>
  <li> The United States Average Home Value over time</li>
  <li> Average ZHVI Over time (by Location)</li>
  <li> Market Heat Over time (by Location)</li>
  <li> Market Heat per month</li>
  <li> ZHVI vs Market Heat (by State)</li>
  <li> ZHVI vs Market Heat (by City)</li>
  <li> Market Heat and Price Mapped (by State)</li>
  <li> Market Heat and Price Mapped (by City)</li>
</ul>


## Future Works
Future goals include:
<ul>
  <li> Predicting the market based on some factors</li>
  <li>Analyzing factors such as square footage, bed and bath, etc. on Home Value</li>
  <li> Inclusion of other Zillow Datasets (as they merge very nicely) </li>
  <li> Publishing this information on the web</li>
</ul>

## Updates:
<div> 12-15-2024: Completed Project </div>

Note - if you came here looking for assignment instructions, go to SupplementaryInfo\CourseInstructions
