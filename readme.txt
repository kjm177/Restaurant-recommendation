Readme for Yelp - restaurant recommendation system

1. Install flask package and run front-end on local machine (Since this is not possible on dumbo)

python RestaurantRecommendation.py


2. Navigate and input queries on the web application 
Assumption: Inputs are always valid userIDs, restaurantIDs and zipcodes
This will generate 2 csv files in the local folder: newReview.csv and searchList.csv

Example valid userIDs: 283421950, 1526931246, -1924967250, 2088196846, -1448218650
Example valid restaurantIDs: -1917383783, 1451962821, 151060605, 952305367, -485226433
Example valid zipcodes: 89104, 89102, 89109, 89119, 89146


3. Put the 2 files generated in step 2 in hdfs
hdfs dfs -put newReview.csv
hdfs dfs -get searchList.csv

4. Run the 3 scala files:
Assumption: Step 3 has been completed
This step will generate a folder: r1

spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
:load data_processing_final.scala
:load ALS_collaborative_filtering.scala
:load getRestaurantLists.scala


5. Load the csv file generated in the r1 folder as the data source into the given tableau worksheet (LasVegas_Recommendations1.twb)

