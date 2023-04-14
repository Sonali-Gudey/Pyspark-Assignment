#Import necessary libraries and modules
from pyspark.sql.types import *
from pyspark.sql.functions import *
from flask import Flask, jsonify
from covid_data import data
from pyspark.sql.functions import sum


data.show(36)
app = Flask(__name__)


#Define the routes and their corresponding functionalities
@app.route('/')
def home():
    return jsonify({'/most_affected_state': "State with the highest death-to-covid ratio.",
    '/least_affected_state': "State with the lowest death-to-covid ratio.",
    '/highest_cases': "State with the highest number of covid cases.",
    '/lowest_cases': "State with the lowest number of covid cases.",
    '/total_cases': "Total number of covid cases.",
    '/most_efficient_state': "State that handled the covid most efficiently (total recoveries/total covid cases).",
    '/least_efficient_state': "State that handled the covid least efficiently (total recoveries/total covid cases).",
    '/getcsvfile':"To export data to csv file at given path"})


#Export data to a csv file at the given path
@app.route('/getcsvfile')
def getcsvfile():
    data.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sonali_gudey/Desktop/spark_results")
    return jsonify({"Message":"Results stored succesfully to '/Users/sonali_gudey/Desktop/spark_results' path"})


#Get the state with the highest death-to-covid ratio
@app.route('/most_affected_state')
def get_most_affected_state():
    most_affected_state=data.sort((data.death.cast("Long")/data.confirm.cast("Long")).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most_affected_state': most_affected_state})


#Get the state with the lowest death-to-covid ratio
@app.route('/least_affected_state')
def get_least_affected_state():
    least_affected_state=data.sort((data.death.cast("Long")/data.confirm.cast("Long"))).select(col("state")).collect()[0][0]
    return jsonify({'least_affected_state': least_affected_state})


#Get the state with the highest number of covid cases
@app.route('/highest_covid_cases')
def get_highest_covid_cases():
    highest_covid_cases=data.sort((data.confirm).cast("Long").desc()).select(col("state")).collect()[0][0]
    return jsonify({'get_highest_covid_cases':highest_covid_cases})


#Get the state with the lowest number of covid cases
@app.route('/least_covid_cases')
def get_least_covid_cases():
    least_covid_cases=data.sort(data.confirm.cast("Long")).select(col("state")).collect()[0][0]
    return jsonify({'get_least_covid_cases':least_covid_cases})


#Get the total number of covid cases
@app.route('/total_cases')
def get_total_cases():
    total_cases=data.select(sum(data.confirm).alias("Total cases")).collect()[0][0]
    return jsonify({'Total Cases':total_cases})


#Get the state that handled the covid most efficiently (total recoveries/total covid cases)
@app.route('/most_efficient_state')
def get_most_efficient_state():
    most_efficient_state=data.sort((data.cured.cast("Long")/data.confirm.cast("Long")).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most efficient_state':most_efficient_state})


#Get the state that handled the covid least efficiently (total recoveries/total covid cases
@app.route('/least_efficient_state')
def get_least_efficient_state():
    least_efficient_state=data.sort((data.cured.cast("Long")/data.confirm.cast("Long")).asc()).select(col("state")).collect()[0][0]
    return jsonify({'least efficient_state':least_efficient_state})


#Running the app
if __name__ == '__main__':
    app.run(debug=True)
