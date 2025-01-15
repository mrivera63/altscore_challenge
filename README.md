# altscore_challenge
This repository attempts to get a low MAE in the Altscore Kaggle Challenge

## Methodology:
This exercise aims to predict the cost of living in various locations across Ecuador by testing four distinct hypotheses:

1. Areas in close proximity have similar living costs.
2. Areas with high mobility intensity have higher living costs.
3. Peak activity hours serve as predictors for living costs.
4. Locations with a high number of recurring visitors tend to have higher living costs.
5. Property prices are related to the living cost in the area.

## Data Used:
- **mobility_data.parquet**: A file capturing movement trends at the device level, [available here](https://www.kaggle.com/competitions/alt-score-data-science-competition/data).
- **ec_properties.csv**: A file containing property characteristics and prices across Ecuador, [available here](https://www.kaggle.com/datasets/rmjacobsen/property-listings-for-5-south-american-countries?select=ec_properties.csv).
