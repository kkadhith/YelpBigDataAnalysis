from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np
from pyspark.sql import SparkSession
import pandas as pd
import json
from pyspark.sql.functions import col, when
import time

spark = SparkSession.builder.appName("CS266Proj").getOrCreate()

business_df = spark.read.json("yelp_dataset/yelp_academic_dataset_business.json")

model_df = business_df[['stars', 'attributes.RestaurantsDelivery', 'attributes.RestaurantsTakeOut', 'attributes.HasTV', 'attributes.GoodForKids', 'attributes.RestaurantsReservations', 'attributes.RestaurantsGoodForGroups', 'attributes.NoiseLevel','attributes.RestaurantsPriceRange2']]

model_df = model_df.dropna()
#replace false and nones with 0
model_df = model_df.withColumn("RestaurantsDelivery", when((col("RestaurantsDelivery") == "None") | (col("RestaurantsDelivery") == "False"), 0).otherwise(col("RestaurantsDelivery")))
model_df = model_df.withColumn("RestaurantsTakeOut", when((col("RestaurantsTakeOut") == "None") | (col("RestaurantsTakeOut") == "False"), 0).otherwise(col("RestaurantsTakeOut")))
model_df = model_df.withColumn("HasTV", when((col("HasTV") == "None") | (col("HasTV") == "False"), 0).otherwise(col("HasTV")))
model_df = model_df.withColumn("GoodForKids", when((col("GoodForKids") == "None") | (col("GoodForKids") == "False"), 0).otherwise(col("GoodForKids")))
model_df = model_df.withColumn("RestaurantsReservations", when((col("RestaurantsReservations") == "None") | (col("RestaurantsReservations") == "False"), 0).otherwise(col("RestaurantsReservations")))
model_df = model_df.withColumn("RestaurantsGoodForGroups", when((col("RestaurantsGoodForGroups") == "None") | (col("RestaurantsGoodForGroups") == "False"), 0).otherwise(col("RestaurantsGoodForGroups")))

model_df = model_df.dropna()
#replace True with 1
model_df = model_df.withColumn("RestaurantsDelivery", when((col("RestaurantsDelivery") == "True"), 1).otherwise(col("RestaurantsDelivery")))
model_df = model_df.withColumn("RestaurantsTakeOut", when((col("RestaurantsTakeOut") == "True"), 1).otherwise(col("RestaurantsTakeOut")))
model_df = model_df.withColumn("HasTV", when((col("HasTV") == "True"), 1).otherwise(col("HasTV")))
model_df = model_df.withColumn("GoodForKids", when((col("GoodForKids") == "True"), 1).otherwise(col("GoodForKids")))
model_df = model_df.withColumn("RestaurantsReservations", when((col("RestaurantsReservations") == "True"), 1).otherwise(col("RestaurantsReservations")))
model_df = model_df.withColumn("RestaurantsGoodForGroups", when((col("RestaurantsGoodForGroups") == "True"), 1).otherwise(col("RestaurantsGoodForGroups")))

business_df.createOrReplaceTempView("business")
query = "SELECT AVG(stars), attributes.NoiseLevel, COUNT(*) FROM business WHERE attributes.NoiseLevel IS NOT NULL AND attributes.NoiseLevel != 'None' GROUP BY attributes.NoiseLevel"
result = spark.sql(query)

model_df = model_df.withColumn("NoiseLevel",when((col("NoiseLevel") == "u'quiet'") | (col("NoiseLevel") == "'quiet'"), "quiet").otherwise(col("NoiseLevel")))
model_df = model_df.withColumn("NoiseLevel",when((col("NoiseLevel") == "u'average'") | (col("NoiseLevel") == "'average'"), "average").otherwise(col("NoiseLevel")))
model_df = model_df.withColumn("NoiseLevel",when((col("NoiseLevel") == "u'loud'") | (col("NoiseLevel") == "'loud'"), "loud").otherwise(col("NoiseLevel")))
model_df = model_df.withColumn("NoiseLevel",when((col("NoiseLevel") == "u'very_loud'") | (col("NoiseLevel") == "'very_loud'"), "very_loud").otherwise(col("NoiseLevel")))

model_df = model_df.filter(model_df["NoiseLevel"] != 'None')

model_df = model_df.withColumn("NoiseLevel", when(col("NoiseLevel") == "quiet", 0).when(col("NoiseLevel") == "average", 1).when(col("NoiseLevel") == "loud", 2).when(col("NoiseLevel") == "very_loud", 3).otherwise(col("NoiseLevel")))

model_df = model_df.withColumn("RestaurantsPriceRange2", when(col("RestaurantsPriceRange2") == "1", 1).when(col("RestaurantsPriceRange2") == "2", 2).when(col("RestaurantsPriceRange2") == "3", 3).when(col("RestaurantsPriceRange2") == "4", 4).otherwise(col("NoiseLevel")))

X = model_df.drop('stars')
y = model_df.select('stars')

X = X.toPandas()
y = y.toPandas()
y = y.iloc[:, 0]

X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=.2, random_state=42)
print("Training the model...")
start2 = time.time()
regr = RandomForestRegressor(n_estimators=50, max_depth=8,random_state=42)
regr.fit(X_train, y_train)
end2 = time.time()
time2 = end2 - start2
test_pred = regr.predict(X_test)
print(f"Model finished training in {round(time2,4)} seconds!")


mse = mean_squared_error(y_test, test_pred)
rmse = np.sqrt(mse)
mae = mean_absolute_error(y_test, test_pred)

print("Mean Squared Error:", mse)
print("Root Mean Squared Error: ", rmse)
print("Mean Absolute Error: ", mae)

#https://stackoverflow.com/questions/37565793/how-to-let-the-user-select-an-input-from-a-finite-list
def get_T_F(prompt):
    response = input(prompt)
    while response not in {"yes", "Yes", "no", "No"}:
        response = input(prompt)
    if response == "no" or response == "No":
        return 0
    else:
        return 1
    
def get_noise_lvl(prompt):
    response = input(prompt)
    noise_lvl = {"quiet": 0, "average": 1, "loud": 2, "very_loud": 3}
    while response not in noise_lvl:
        response = input(prompt)
    return noise_lvl[response]

def get_price_range(prompt):
    response = input(prompt)
    while response not in {"1", "2", "3", "4"}:
        response = input(prompt)
    return int(response)

def userInput():
    user_input = {
       'RestaurantsDelivery': get_T_F("Does your restaurant offer delivery? Please enter yes or no: "), 
       'RestaurantsTakeOut': get_T_F("Does your restaurant offer take out? Please enter yes or no: "),
       'HasTV': get_T_F("Does your restaurant have a TV(s)? Please enter yes or no: "),
       'GoodForKids': get_T_F("Is your restaurant good for kids? Please enter yes or no: "),
       'RestaurantsReservations': get_T_F("Does your restaurant allow guests to make reservations? Please enter yes or no: "), 
       'RestaurantsGoodForGroups': get_T_F("Is your restaurant good for groups? Please enter yes or no: "), 
       'NoiseLevel':get_noise_lvl("What is the noise level of your restaurant? Please enter quiet, average, loud, or very_loud: ") ,
       'RestaurantsPriceRange2': get_price_range("What is the price range? Please enter: 1, 2, 3, or 4 ($,$$,$$$,$$$$): ") 
    }
    return user_input

user_input = userInput()

input_df = pd.DataFrame([user_input])
start = time.time()
query_pred = regr.predict(input_df)
print(f"Based on these features, we predict you will have a {round(query_pred[0], 2)} rating!")
end = time.time()
time = end-start
print(f"Our model took {round(time,4)} to predict your restuarant rating.")
