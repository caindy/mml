# Project Goal: Medium-Term Load Forecast

The MISO MTLF is updated every 15 minutes and forecasts hourly peak load for
seven days. The MTLF values reported against actual load are fixed every
24-hours at midnight. Therefore validation and testing of the model should
exercise walk-forward techniques that predict daily-aligned 24-hour sequences.

<!-- What is it we're trying to achieve https://misortwd.azurewebsites.net/MISORTWDDataBroker/DataBrokerServices.asmx?messageType=gettotalload&returnType=csv -->

## Public Data

For myriad reasons, the inputs to the MISO historical MTLF models are not
available, but as a time series prediction model, it should minimally use
up-to-date forecasts of weather data as well as current load data. This project
shall only utilize publicly available data that is free to use.

* [Historical Weather Data](https://mesonet.agron.iastate.edu/ASOS/): "The Iowa
  Environmental Mesonet (IEM) collects environmental data from cooperating members
  with observing networks. [...] The Automated Surface Observing System (ASOS) is
  considered to be the flagship automated observing network."
* [MISO Market Reports](https://www.misoenergy.org/markets-and-operations/real-time--market-data/market-reports)

A particular gap in the available data is high quality weather forecasts.



### Population Density Heuristic

![Redditor US Population Density By County](https://i.redd.it/6azaarhnj8111.png)
([source](https://www.reddit.com/r/dataisbeautiful/comments/8nkwii/population_density_of_the_us_by_county_updated_oc/))
