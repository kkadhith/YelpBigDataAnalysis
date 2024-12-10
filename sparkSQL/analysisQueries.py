from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CS266Proj").getOrCreate()
import pandas as pd
import json
from pyspark.sql.functions import col, when


business_df = spark.read.json("yelp_dataset\yelp_academic_dataset_business.json")
reviews_df = spark.read.json("yelp_dataset\yelp_academic_dataset_review.json")

business_df.createOrReplaceTempView("business")

reviews_df.createOrReplaceTempView("Reviews")

#query to fetch distribution of review reactions per state
query = "SELECT B.state, SUM(R.cool) as cool, SUM(R.funny) AS funny, SUM(R.useful) AS useful FROM Reviews R, Business B WHERE R.business_id = B.business_id GROUP BY B.state ORDER BY cool DESC"
ratingDistPerState = spark.sql(query)

ratingDistPerState.show()
#query to fetch distribution of review reactions across every state combined
query = "SELECT SUM(cool) AS cool, SUM(funny) AS funny, SUM(useful) AS useful FROM Reviews"
ratingDist = spark.sql(query)

ratingDist.show()

#query to fetch average star rating per state
query = "SELECT state, AVG(stars) AS avgStars FROM business GROUP BY state"
avgStarsPerState = spark.sql(query)

avgStarsPerState.show()

#prepping new dataframe to use for analysis of most common attribute for high-rated restaurants per state
model_df = business_df[['state', 'stars', 'attributes.RestaurantsDelivery', 'attributes.RestaurantsTakeOut', 'attributes.HasTV', 'attributes.GoodForKids', 'attributes.RestaurantsReservations', 'attributes.RestaurantsGoodForGroups', 'attributes.NoiseLevel']]
model_df.show()
model_df.count()

updated = model_df.dropna()
updated.show()
updated.count()

updated = updated.withColumn("RestaurantsDelivery", when(col("RestaurantsDelivery") == "None", "False").otherwise(col("RestaurantsDelivery")))
updated = updated.withColumn("RestaurantsTakeOut", when(col("RestaurantsTakeOut") == "None", "False").otherwise(col("RestaurantsTakeOut")))
updated = updated.withColumn("HasTV", when(col("HasTV") == "None", "False").otherwise(col("HasTV")))
updated = updated.withColumn("GoodForKids", when(col("GoodForKids") == "None", "False").otherwise(col("GoodForKids")))
updated = updated.withColumn("RestaurantsReservations", when(col("RestaurantsReservations") == "None", "False").otherwise(col("RestaurantsReservations")))
updated = updated.withColumn("RestaurantsGoodForGroups", when(col("RestaurantsGoodForGroups") == "None", "False").otherwise(col("RestaurantsGoodForGroups")))
updated.show()
updated.count()

updated.createOrReplaceTempView("updated")


#query that gets the top restaurant attribute in each state for high-rated restaurants
#it counts the total number of trues for each attribute in question, and picks the max per state
#duplicates are also handled (when there is a tie between two or more attributes)
query="""
WITH AttributeCounts AS (
    SELECT 
        state, 
        'RestaurantsDelivery' AS attribute, SUM (CASE WHEN RestaurantsDelivery = 'True' AND stars >= 4  THEN 1 ELSE 0 END) AS numTrue FROM updated GROUP BY state
    UNION ALL
    SELECT 
        state, 
        'RestaurantsTakeOut' AS attribute, SUM (CASE WHEN RestaurantsTakeOut = 'True' AND stars >= 4  THEN 1 ELSE 0 END) AS numTrue FROM updated GROUP BY state
    UNION ALL
    SELECT 
        state, 
        'HasTV' AS attribute, SUM (CASE WHEN HasTV = 'True' AND stars >= 4  THEN 1 ELSE 0 END) AS numTrue FROM updated GROUP BY state
    UNION ALL
    SELECT 
        state, 
        'GoodForKids' AS attribute, SUM (CASE WHEN GoodForKids = 'True' AND stars >= 4 THEN 1 ELSE 0 END) AS numTrue FROM updated GROUP BY state
    UNION ALL
    SELECT 
        state, 
        'RestaurantsReservations' AS attribute, SUM (CASE WHEN RestaurantsReservations = 'True' AND stars >= 4  THEN 1 ELSE 0 END) AS numTrue FROM updated GROUP BY state
    UNION ALL
    SELECT 
        state, 
        'RestaurantsGoodForGroups' AS attribute, SUM (CASE WHEN RestaurantsGoodForGroups = 'True' AND stars >= 4  THEN 1 ELSE 0 END) AS numTrue FROM updated GROUP BY state
),
FinalAttributeCounts AS (
    SELECT 
        state, 
        attribute, 
        numTrue, 
        ROW_NUMBER() OVER (PARTITION BY state ORDER BY numTrue DESC, attribute ASC) AS seen 
    FROM AttributeCounts
)
SELECT state, attribute 
FROM FinalAttributeCounts 
WHERE seen = 1

"""

topAttributePerState = spark.sql(query)

topAttributePerState.show()


#creates dataframe that JOINs the perState dataframes calculated above, joins on state so we can use this in front end
json_df = (ratingDistPerState.join(avgStarsPerState, "state")).join(topAttributePerState, "state")

json_df.show()

#generates JSON file that is used in frontend code for interactive map
json_df.toPandas().to_json('analysis.json', orient='index')