#Importing required libraries
import requests
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.sql import SparkSession
from config import KEY 
from pyspark.sql.functions import regexp_replace


#Function to get the data from API and return the response object
def get_data():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"
    headers = {
        "X-RapidAPI-Key": KEY,
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers)
    return response


#Function to clean the given dataframe
#Cleaning involves removing corrupted records, empty/void records,and stripping unwanted parts in state names
def clean_data(data):
    # Drop the '_corrupt_record' column
    data=data.drop('_corrupt_record')
    # Filter out records where 'state' is null or void
    data=data.where((data.state.isNotNull()) & (data.state!=''))
    data = data.withColumn("confirm",col("confirm").cast("Long")).withColumn("cured",col("cured").cast("Long")).withColumn("death",col("death").cast("Long"))
    data=data.select("slno","state","confirm","cured","death","total")
    # Strip state names ending with '*'
    data=data.withColumn('state', regexp_replace('state', '\*',""))
    return data


#Create a Spark session
spark = SparkSession.builder.master('local[*]').getOrCreate() 
sc = SparkContext.getOrCreate() 
sc.setLogLevel("ERROR")
#Get response object from the API using the 'get_data' function
response = get_data()
#Create a dataframe from the RDD
json_rdd = sc.parallelize(response.json().values()) 
data = spark.read.json(json_rdd) 
data = clean_data(data)
