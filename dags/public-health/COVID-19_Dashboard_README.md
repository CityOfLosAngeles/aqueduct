# City of LA COVID-19 Dashboard README
The Johns Hopkins Center for Systems Science and Engineering has open sourced data culled from the US CDC, World Health Organization, DXY (China), China CDC (China), Hong Kong Department of Health, Macau Government, Taiwan CDC, European CDC, Government of Canada, Australia Government Department of Health, and other local, state, and regional health authorities. The team at JHU has [written a blog](https://systems.jhu.edu/research/public-health/ncov/) about their efforts in providing real-time information in the face of a global public health emergency.

## Important JHU Source Materials
* [JHU Dashboard](https://www.arcgis.com/apps/opsdashboard/index.html#/bda7594740fd40299423467b48e9ecf6)
* [JHU ESRI feature layer](https://www.arcgis.com/home/webmap/viewer.html?layers=c0b356e20b30490c8b8b4c7bb9554e7c)
* [JHU ESRI feature service](https://www.arcgis.com/home/item.html?id=c0b356e20b30490c8b8b4c7bb9554e7c)
* [JHU COVID-19 GitHub repo](https://github.com/CSSEGISandData/COVID-19)

The City of LA initially built a dashboard pulling data directly from JHU's ESRI feature service, but since 3/10/2020, JHU changed the underlying data published from city/county level to state level. To meet the City of LA's own dashboard needs, we used Aqueduct, our shared pipeline for building ETLs and scheduling batch jobs, to make available the original city/county level data.

**3/13/2020 update:** JHU's CSVs will be at the state level, and not at the city/county level anymore, [as noted in their GitHub issue](https://github.com/CSSEGISandData/COVID-19/issues/382). Since JHU's feature layers weren't connecting to our dashboard, we adapted our ETL to continue to grab province/state level data for the world and will publish these as 2 public ESRI feature layers (#1, #2).

In addition, we are scraping the websites for Southern California counties belonging in the Southern California Association of Governments (SCAG) region. We have data from Los Angeles, Orange County, and Imperial Counties. In the coming days, we will add Ventura, Riverside, and San Bernardino Counties. We have combined JHU county data up to 3/12/2020 with our own compilation of counts, and will publish these as 2 public ESRI feature layers (#3, #4).

1. Worldwide time-series data available at the state level of confirmed cases, deaths, and recovered.
2. Worldwide *current date's* data available at the state level of confirmed cases, deaths, and recovered.
3. SCAG Region time-series data available at the county level of confirmed cases, travel-related cases, and community spread cases. Not all counties report the breakdown due to travel vs. community spread, but we grab it if it's available.
4. SCAG Region *current date's* data available at the county level of confirmed cases, recovered, travel-related cases, and community spread cases.

## Important City of LA Source Materials
* [City of LA COVID-19 Dashboard](https://lahub.maps.arcgis.com/apps/opsdashboard/index.html#/82b3434c38ac4fad80cc281efbeb96ca)
* [Worldwide state level time-series feature layer](http://lahub.maps.arcgis.com/home/item.html?id=20271474d3c3404d9c79bed0dbd48580)
* [Worldwide state level current date's feature layer](http://lahub.maps.arcgis.com/home/item.html?id=191df200230642099002039816dc8c59)
* [SCAG Region county level time-series feature layer](http://lahub.maps.arcgis.com/home/item.html?id=d61924e1d8344a09a1298707cfff388c)
* [SCAG Region county level current date's feature layer](http://lahub.maps.arcgis.com/home/item.html?id=523a372d71014bd491064d74e3eba2c7)
* [City of LA's COVID-19 ETL](https://github.com/CityOfLosAngeles/aqueduct/tree/master/dags/public-health)


We believe that open source data will allow policymakers and local authorities to monitor a rapidly changing situation. It will prevent other entities from "reinventing the wheel", and we welcome collaboration and pull requests on our work!


### Prior Updates to Workflow
**3/12/2020 update:** JHU publishes new CSVs daily with city/county level counts for the world. Starting from 3/10/2020, the JHU dataset includes both county-level and state-level observations. [JHU GitHub issue discussing this change.](https://github.com/CSSEGISandData/COVID-19/issues/559) We schedule our ETL around these CSVs made available on GitHub and repackage them into 2 public ESRI feature layers:
1. Time-series data available at the city/county level of confirmed cases, deaths, and recovered.
2. The *current date's* city/county data of confirmed cases, deaths. In the coming days, we hope to update or publish a new feature layer that contains the state's and country's total cases, deaths, and recovered.


### Disclaimer
We are using the Johns Hopkins data for our ETL and ESRI feature services. Their disclaimer is below:

This website and its contents herein, including all data, mapping, and analysis (“Website”), copyright 2020 Johns Hopkins University, all rights reserved, is provided to the public strictly for educational and academic research purposes. The Website relies upon publicly available data from multiple sources, that do not always agree. The names of locations correspond with the official designations used by the U.S. State Department, including for Taiwan. The Johns Hopkins University hereby disclaims any and all representations and warranties with respect to the Website, including accuracy, fitness for use, and merchantability. Reliance on the Website for medical guidance or use of the Website in commerce is strictly prohibited.


### Contributors
* Hunter Owens
* Ian Rose
* Tiffany Chu
