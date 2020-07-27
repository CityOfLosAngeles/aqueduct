# City of LA COVID-19 DAGs README

This contains all scheduled jobs for data sources pertaining to COVID-19.
At present, all datasets except for the GetHelp data tracking emergency
homeless shelters have been retired.

## Contents
1. [Homeless Shelter Data](#homeless-shelter-data)
1. [Prior Updates to Workflow](#prior-updates-to-workflow)
1. [Helpful Hints for Jupyter Notebooks](#helpful-hints)

## Homeless Shelter Data

* [Shelter time-series feature layer](https://lahub.maps.arcgis.com/home/item.html?id=bd17014f8a954681be8c383acdb6c808) - a time-series of historical bed count data for all of the active shelters.

* [Current shelter feature layer](https://lahub.maps.arcgis.com/home/item.html?id=dd618cab800549358bac01bf218406e4) - a snapshot of the current status of bed counts for the shelters. Some shelters may be listed that are not yet active, and have inaccurate information. You can remove them by filtering for `status != 0`.

* [Shelter stats](https://lahub.maps.arcgis.com/home/item.html?id=9db2e26c98134fae9a6f5c154a1e9ac9) - some aggregate statistics of the current shelter status, including number of unique shelters, number with known status, and number of available beds.

* The relevant script to transform shelter data is: `get-help-to-esri.py`.


The City uses a shelter data management to a system run by [GetHelp](https://gethelp.com). This includes bed counts, shelter service information, and historical data. The DAG `get-help-to-esri.py` loads shelter data from the GetHelp shelter management platform and uploads it to ESRI for GIS analysis and dashboarding. API documentation for the GetHelp system can be found in
[this Google spreadsheet](https://docs.google.com/spreadsheets/d/1n08OgJ6BCFnbSxGMyY9v9vDFN8jqz_xgZEk7x7-vUUA/edit?usp=sharing/#gid=864625912). Our ESRI map layers are public and listed in the [Data Sources section](#data-sources).


## Prior Updates to Workflow
### COVID-19 Cases
**7/27/2020 update:** All DAGs except for the GetHelp one have been retired.

**4/1/2020 update:** To reconcile the multiple schemas from JHU and NYT for our US table, we use Aqueduct, our shared pipeline for building ETLs and scheduling batch jobs.

* **US:** Use NYT county-level time-series data up through 3/31. Then, schedule a job that pulls JHU county-level time-series data (which is updated hourly). Append those into one time-series dataset and calculate state totals.

**3/13/2020 update:** JHU's CSVs will be at the state level, and not at the city/county level anymore, [as noted in their GitHub issue](https://github.com/CSSEGISandData/COVID-19/issues/382). Since JHU's feature layers weren't connecting to our dashboard, we adapted our ETL to continue to grab province/state level data for the world and will publish these as 2 public ESRI feature layers (#1, #2). Our ETL checks JHU data ***every hour***.

In addition, we are scraping the websites for Southern California counties belonging in the Southern California Association of Governments (SCAG) region. We have data from [Los Angeles](http://publichealth.lacounty.gov/media/Coronavirus/), [Orange County](http://www.ochealthinfo.com/phs/about/epidasmt/epi/dip/prevention/novel_coronavirus), and [Imperial](http://www.icphd.org/health-information-and-resources/healthy-facts/covid-19/) Counties. In the coming days, we will add [Ventura](https://www.ventura.org/covid19/), [Riverside](https://www.rivcoph.org/coronavirus), and [San Bernardino](http://wp.sbcounty.gov/dph/coronavirus/) Counties. We have combined JHU county data up to 3/12/2020 with our own compilation of counts, and will publish these as 2 public ESRI feature layers (#3, #4). Our ETL scrapes case counts published by county public health agency websites ***every hour***.

1. Worldwide time-series data available at the state level of confirmed cases, deaths, and recovered.
2. Worldwide *current date's* data available at the state level of confirmed cases, deaths, and recovered.
3. SCAG Region time-series data available at the county level of confirmed cases, travel-related cases, and community spread cases. Not all counties report the breakdown due to travel vs. community spread, but we grab it if it's available.
4. SCAG Region *current date's* data available at the county level of confirmed cases, travel-related cases, and community spread cases.

**3/12/2020 update:** JHU publishes new CSVs daily with city/county level counts for the world. Starting from 3/10/2020, the JHU dataset includes both county-level and state-level observations. [JHU GitHub issue discussing this change.](https://github.com/CSSEGISandData/COVID-19/issues/559) We schedule our ETL around these CSVs made available on GitHub and repackage them into 2 public ESRI feature layers:
1. Time-series data available at the city/county level of confirmed cases, deaths, and recovered.
2. The *current date's* city/county data of confirmed cases, deaths. In the coming days, we hope to update or publish a new feature layer that contains the state's and country's total cases, deaths, and recovered.


## Helpful Hints

Jupyter Notebooks can read in both the ESRI feature layer and the CSV. In our [Data Sources](#data-sources), we often provide links to the ESRI feature layer and CSV. More in our [COVID-19 indicators GitHub repo](https://github.com/CityOfLosAngeles/covid19-indicators).

Ex: JHU global province-level time-series [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=20271474d3c3404d9c79bed0dbd48580) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=daeef8efe43941748cb98d7c1f716122)


**Import the CSV**

All you need is the item ID of the CSV item. We use an f-string to construct the URL and use Python `pandas` package to import the CSV.

```
JHU_GLOBAL_ITEM_ID = "daeef8efe43941748cb98d7c1f716122"

JHU_URL = f"http://lahub.maps.arcgis.com/sharing/rest/content/items/{JHU_GLOBAL_ITEM_ID}/data"

import pandas as pd
df = pd.read_csv(JHU_URL)
```

**Import ESRI feature layer**

* From the feature layer, click on `service URL`.
* Scroll to the bottom and click `Query`
* Fill in the following parameters:
    * WHERE: 1=1
    * Out Fields (fill in the list of columns to retrieve): Province_State, Country_Region, Lat, Long, date, number_of_cases, number_of_deaths, number_of_recovered, ObjectId
    * Format: GeoJSON
    * Query (GET)
* Now, grab the new URL (it should be quite long), and read in that URL through `geopandas`. Note: the ESRI date field is less understandable, and converting it to pandas datetime will be incorrect.

```
FEATURE_LAYER_URL = "http://lahub.maps.arcgis.com/home/item.html?id=20271474d3c3404d9c79bed0dbd48580"

SERVICE_URL = "https://services5.arcgis.com/7nsPwEMP38bSkCjy/arcgis/rest/services/jhu_covid19_time_series/FeatureServer/0"

CORRECT_URL = "https://services5.arcgis.com/7nsPwEMP38bSkCjy/arcgis/rest/services/jhu_covid19_time_series/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=Province_State%2C+Country_Region%2C+Lat%2C+Long%2C+date%2C+number_of_cases%2C+number_of_deaths%2C+number_of_recovered%2C+ObjectId&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="


import geopandas as gpd
gdf = gpd.read_file(CORRECT_URL)
```


## Contributors
* [Hunter Owens](https://github.com/hunterowens)
* [Ian Rose](https://github.com/ian-r-rose)
* [Tiffany Chu](https://github.com/tiffanychu90)
* [Brendan Bailey](https://github.com/brendanbailey)
