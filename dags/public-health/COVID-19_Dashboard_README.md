# City of LA COVID-19 Dashboard README
The Johns Hopkins Center for Systems Science and Engineering has open sourced data culled from the US CDC, World Health Organization, DXY (China), China CDC (China), Hong Kong Department of Health, Macau Government, Taiwan CDC, European CDC, Government of Canada, Australia Government Department of Health, and other local, state, and regional health authorities. The team at JHU has [written a blog](https://systems.jhu.edu/research/public-health/ncov/) about their efforts in providing real-time information in the face of a global public health emergency.

## Important JHU source materials
* [JHU Dashboard](https://www.arcgis.com/apps/opsdashboard/index.html#/bda7594740fd40299423467b48e9ecf6)
* [JHU ESRI feature layer](https://www.arcgis.com/home/webmap/viewer.html?layers=c0b356e20b30490c8b8b4c7bb9554e7c)
* [JHU ESRI feature service](https://www.arcgis.com/home/item.html?id=c0b356e20b30490c8b8b4c7bb9554e7c)
* [JHU COVID-19 GitHub repo](https://github.com/CSSEGISandData/COVID-19)

The City of LA initially built a dashboard pulling data directly from JHU's ESRI feature service, but since 3/10/2020, JHU changed the underlying data published from city/county level to state level. To meet the City of LA's own dashboard needs, we used Aqueduct, our shared pipeline for building ETLs and scheduling batch jobs, to make available the original  city/county level data.

JHU publishes new CSVs daily with city/county level counts for the world. We schedule our ETL around these CSVs made available on GitHub and repackage them into 2 public ESRI feature layers:
1. Time-series data available at the city/county level of confirmed cases, deaths, and recovered.
2. The *current date's* city/county data of confirmed cases, deaths, and recovered and state and country totals for cases, deaths, and recovered.

## Important City of LA source materials:
* [City of LA COVID-19 Dashboard](https://lahub.maps.arcgis.com/apps/opsdashboard/index.html#/82b3434c38ac4fad80cc281efbeb96ca)
* Time-series feature layer
* Current date's feature layer with state and country totals
* [City of LA's COVID-19 ETL](https://github.com/CityOfLosAngeles/aqueduct/tree/master/dags/public-health)

We believe that open source data will allow policymakers and local authorities to monitor a rapidly changing situation. It will prevent other entities from "reinventing the wheel", and we welcome collaboration and pull requests on our work!

### Disclaimer
We rely on Johns Hopkins data, which in turn relies on a compilation of multiple, sometimes conflicting, sources. While JHU does perform manual validation of information readily made public, data available during an evolving crisis is always a "best guess" of the unknown. However, it is crucial to have a single source of the "best guess", and we know most US cities and media are using JHU data. Nevertheless, reliance on this site and its contents herein, including data, mapping, and analysis for medical guidance or commerce is strictly prohibited.


### Contributors
* Hunter Owens
* Ian Rose
* Tiffany Chu
