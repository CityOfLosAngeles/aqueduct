# City of LA COVID-19 Dashboard README

## Contents
We've documented all the data that feeds into the City of LA COVID-19 Dashboard and listed each source in [Section 1: Data Sources](#data-sources). We believe that open source data will allow policymakers and local authorities to monitor a rapidly changing situation and avoid the need to "reinvent the wheel". For more technical information on our methodology and workflow, please read through Sections 2-6.

1. [Data Sources](#data-sources)
1. [COVID-19 Case Data](#covid-19-case-data)
1. [Homeless Shelter Data](#homeless-shelter-data)
1. [Hospital Bed and Equipment Availability Data](#hospital-bed-and-equipment-availability-data)
1. [COVID-19 Testing Data](#testing-data)
1. [Prior Updates to Workflow](#prior-updates-to-workflow)
1. [Helpful Hints for Jupyter Notebooks](#helpful-hints)


## Data Sources
* City of LA COVID-19 Dashboard: [Desktop Version](https://arcg.is/0WqSmb) and [Mobile Version](https://arcg.is/0yD90W0) and [FAQs](https://docs.google.com/document/d/1U96_d1LTabeWl6uZv97ZEVKaKYse_UdebzO_6gAfNAc/)

* [City of LA's COVID-19 GitHub repo](https://github.com/CityOfLosAngeles/aqueduct/tree/master/dags/public-health/covid19/). We use Aqueduct, our shared pipeline for building ETLs and scheduling batch jobs. We welcome collaboration and pull requests on our work!

#### COVID-19 Cases
* Global province-level time-series [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=20271474d3c3404d9c79bed0dbd48580) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=daeef8efe43941748cb98d7c1f716122)

* Global province-level current date's [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=191df200230642099002039816dc8c59) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=6f3f214220f443b2beed8d1374b02cf7)

* US county-level time-series [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=4e0dc873bd794c14b7bd186b4b5e74a2) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=8aba663239fc428f8bcdc48e213e3172)

* Comparison of metropolitan infection rates [feature table](http://lahub.maps.arcgis.com/home/item.html?id=b37e229b71dc4c65a479e4b5912ded66) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=27efb06ce2954b90ae833dabb570b1cf). The [MSA to county crosswalk](https://github.com/CityOfLosAngeles/aqueduct/blob/master/dags/public-health/covid19/msa_county_pop_crosswalk.csv) was derived from the [National Bureau of Economic Research crosswalk](https://data.nber.org/data/cbsa-msa-fips-ssa-county-crosswalk.html) using `make-crosswalk.py`.

* LA County Dept of Public Health (DPH) neighborhood-level current date's [feature layer](https://lahub.maps.arcgis.com/home/item.html?id=ca30397902484e9c911e8092788a0233)

* City of LA case count time-series [feature table](https://lahub.maps.arcgis.com/home/item.html?id=1d1e4679a94e43e884b97a0488fc04cf) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=7175fba373f541a7a19df56b6a0617f4)

* The relevant scripts are: `jhu-to-esri.py`, `jhu-county-to-esri.py`, and `sync-la-cases-data.py`.

#### Shelters
* [Shelter time-series feature layer](https://lahub.maps.arcgis.com/home/item.html?id=bd17014f8a954681be8c383acdb6c808) - a time-series of historical bed count data for all of the active shelters.

* [Current shelter feature layer](https://lahub.maps.arcgis.com/home/item.html?id=dd618cab800549358bac01bf218406e4) - a snapshot of the current status of bed counts for the shelters. Some shelters may be listed that are not yet active, and have inaccurate information. You can remove them by filtering for `status != 0`.

* [Shelter stats](https://lahub.maps.arcgis.com/home/item.html?id=9db2e26c98134fae9a6f5c154a1e9ac9) - some aggregate statistics of the current shelter status, including number of unique shelters, number with known status, and number of available beds.

* The relevant script to transform shelter data is: `get-help-to-esri.py`.

#### Hospital Bed and Equipment Availability
* Hospital bed and equipment availability [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=956e105f422a4c1ba9ce5d215b835951) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=3da1eb3e13a14743973c96b945bd1117)

#### COVID-19 Testing
* City of LA COVID-19 tests administered [feature layer](https://lahub.maps.arcgis.com/home/item.html?id=64b91665fef4471dafb6b2ff98daee6c) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=158dab4a07b04ecb8d47fea1746303ac)

* The relevant script is to transform testing data is: `sync-covid-testing-data.py`.


## COVID-19 Case Data
The Johns Hopkins Center for Systems Science and Engineering has open sourced data culled from the US CDC, World Health Organization, DXY (China), China CDC (China), Hong Kong Department of Health, Macau Government, Taiwan CDC, European CDC, Government of Canada, Australia Government Department of Health, and other local, state, and regional health authorities. The team at JHU has [written a blog](https://systems.jhu.edu/research/public-health/ncov/) about their efforts in providing real-time information in the face of a global public health emergency.

JHU initially published US county-level data until 3/10/2020. On 3/10, JHU started publishing only US state level data ([GitHub issue](https://github.com/CSSEGISandData/COVID-19/issues/382)). As a result, there is a major gap in JHU's county time-series data between 3/10-3/23, until they started publishing county-level data again on 3/24 ([GitHub issue](https://github.com/CSSEGISandData/COVID-19/issues/1250)). In April, JHU provided historical county time-series data going back to January in their GitHub.


### Important JHU and Other Source Materials
* [JHU Dashboard](https://www.arcgis.com/apps/opsdashboard/index.html#/bda7594740fd40299423467b48e9ecf6)

* [JHU ESRI feature layers for global province and country data](https://www.arcgis.com/home/item.html?id=c0b356e20b30490c8b8b4c7bb9554e7c)

* [JHU ESRI feature layer for US county data](https://www.arcgis.com/home/item.html?id=628578697fb24d8ea4c32fa0c5ae1843) and [blog post](https://www.esri.com/arcgis-blog/products/product/public-safety/coronavirus-covid-19-data-available-by-county-from-johns-hopkins-university/)

* [JHU COVID-19 GitHub repo](https://github.com/CSSEGISandData/COVID-19)

* [JHU geography lookup table](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv)

* [New York Times COVID-19 GitHub repo](https://github.com/nytimes/covid-19-data)

* [LA Times COVID-19 GitHub repo with CA county-level data](https://github.com/datadesk/california-coronavirus-data)


### City of LA Workflow
**4/9/2020 update:** We create one table for the US and one for the rest of the world, called *global*. The historical time-series is pulled from JHU's [CSV on GitHub](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series) and appended with the current date's data from the ESRI feature layer.

* **Global:** Use JHU province-level time-series data. The US is a singular observation as a country. Smaller countries report only country-level data, while larger countries like China, Australia, and Canada include province-level data.
* **US:** Use JHU's US county-level time-series data and calculate state totals and change in cases and deaths from prior day.
* **MSA Comparison:** An automatic comparison of number of cases per million for key metropolitan statistical areas (MSAs) to power our chart in the dashboard. These key MSAs (and CBSA FIPS codes) are: Los Angeles/Orange County (31080), San Francisco/San Jose (41860, 41940), New York City (35620), Seattle (42660), and Detroit (19820).

Additionally, a [Google spreadsheet](https://docs.google.com/spreadsheets/d/1Vk7aGL7O0ZVQRySwh6X2aKlbhYlAR_ppSyMdMPqz_aI/) is manually maintained for the number of cases that fall within the City of LA. Since the City of LA falls within LA County, the LA County Department of Public Health reports neighborhood breakdowns of case counts, with a subtotal for the City of LA. We sync the Google spreadsheet with our ESRI layer in the `sync-la-cases-data.py` DAG.

Our ETLs check JHU data ***every hour***. Our  Our ESRI map layers are public and listed in the [Data Sources section](#data-sources).


### Disclaimer
We are using the Johns Hopkins University data for our ETL and ESRI feature services. Their disclaimer is below:

This website and its contents herein, including all data, mapping, and analysis (“Website”), copyright 2020 Johns Hopkins University, all rights reserved, is provided to the public strictly for educational and academic research purposes. The Website relies upon publicly available data from multiple sources, that do not always agree. The names of locations correspond with the official designations used by the U.S. State Department, including for Taiwan. The Johns Hopkins University hereby disclaims any and all representations and warranties with respect to the Website, including accuracy, fitness for use, and merchantability. Reliance on the Website for medical guidance or use of the Website in commerce is strictly prohibited.


## Homeless Shelter Data
The City uses a shelter data management to a system run by [GetHelp](https://gethelp.com). This includes bed counts, shelter service information, and historical data. The DAG `get-help-to-esri.py` loads shelter data from the GetHelp shelter management platform and uploads it to ESRI for GIS analysis and dashboarding. API documentation for the GetHelp system can be found in
[this Google spreadsheet](https://docs.google.com/spreadsheets/d/1n08OgJ6BCFnbSxGMyY9v9vDFN8jqz_xgZEk7x7-vUUA/edit?usp=sharing/#gid=864625912). Our ESRI map layers are public and listed in the [Data Sources section](#data-sources).


## Hospital Bed and Equipment Availability Data
LA County issues a [daily HavBed pdf survey](http://file.lacounty.gov/SDSInter/dhs/1070069_HavBedSummary.pdf) on the number of beds and ventilators that are available, unavailable, or occupied by COVID-19 patients. This survey is manually entered into a [Google spreadsheet](https://docs.google.com/spreadsheets/d/1rS0Vt-kuxwQKoqZBcaOYOOTc5bL1QZqAqqPSyCaMczQ/edit?usp=sharing) and updated via the `sync-bed-availability-data.py` DAG. Our ESRI map layers are public and listed in the [Data Sources section](#data-sources).


## Testing Data
The City collects data on the number of tests performed and test kits available from its several COVID-19 testing sites. The DAG `sync-covid-testing-data.py` syncs the data from a [Google spreadsheet](https://docs.google.com/spreadsheets/d/1agPpAJ5VNqpY50u9RhcPOu7P54AS0NUZhvA2Elmp2m4/edit?usp=sharing) with our ESRI feature layer. Our ESRI map layers are public and listed in the [Data Sources section](#data-sources).


## Prior Updates to Workflow
### COVID-19 Cases
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

### Shelter Data
The DAG `shelter-to-esri.py` takes the Rec & Parks (RAP) shelter census (collected at 8 intervals a day) through a Google Form and pushes it into the City of LA GeoHub by merging it with the the official shelter data from LA Sanitation (LASAN) and RAP GIS staff. The report intervals are: 6:30am, 10:30am, 11:30am, 12:30pm, 1:30pm, 2:30pm, 3:30pm, 4:30pm, and 8:30pm. We do some timezone data cleaning and publish.

`Timestamp` is the time in which the shelter actually submitted the Google form. `Date` and `Time` are which "report" they are filing for.

Note, the capacity numbers should be calculated by `sum(occupied beds + unoccupied beds)`, rather than the normal capacity, which has been lower to help adhere to social distancing in the shelters.


## Helpful Hints
Jupyter Notebooks can read in both the ESRI feature layer and the CSV. In our [Data Sources](#data-sources), we often provide links to the ESRI feature layer and CSV.

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
