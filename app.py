# Importing the required imports
from pyspark.sql.types import *
from pyspark.sql.functions import *
from flask import Flask, jsonify
from covid_data import data # Fecthing the dataframe created in other file.
from pyspark.sql.functions import sum
 
print("The data after cleaning and creating dataframe :-\n")
data.show(36) # Displaying the above fetched dataframe
app = Flask(__name__) # creating a app from flask
@app.route('/') # defing the things to happen on home path
def home():
    # returning the jsonfied index
    return jsonify({'/most_affected_state': "State with the highest death-to-covid ratio.",
                    '/least_affected_state': "State with the lowest death-to-covid ratio.",
                    '/highest_cases': "State with the highest number of covid cases.",
                    '/lowest_cases': "State with the lowest number of covid cases.",
                    '/total_cases': "Total number of covid cases.",
                    '/most_efficient_state': "State that handled the covid most efficiently (total recoveries/total covid cases).",
                    '/least_efficient_state': "State that handled the covid least efficiently (total recoveries/total covid cases).",
                    '/getcsvfile':"To export data to csv file at given path"
                    })

@app.route('/getcsvfile') # define the things to happen on /getcsvfile path
def getcsvfile():
    # Exporting the data into csv as single file
    data.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sonali_gudey/Desktop/spark_results")
    return jsonify({"Message":"Results stored succesfully to '/Users/sonali_gudey/Desktop/spark_results' path"}) # returning the success meassage jsonfied response
  
@app.route('/most_affected_state') # defing the things to happen on /most_affected_state
def get_most_affected_state():
    # Sorting the data frame as per given criteria in descending and selecting the top most record and state column
    most_affected_state=data.sort((data.death.cast("Long")/data.confirm.cast("Long")).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most_affected_state': most_affected_state})  # returning the jsonfied response

@app.route('/least_affected_state') # defing the things to happen on /least_affected_state
def get_least_affected_state():
    # Sorting the data frame as per given criteria in ascending and selecting the top most record and state column
    least_affected_state=data.sort((data.death.cast("Long")/data.confirm.cast("Long"))).select(col("state")).collect()[0][0]
    return jsonify({'least_affected_state': least_affected_state}) # returning the jsonfied response

@app.route('/highest_covid_cases') # defing the things to happen on /highest_covid_cases
def get_highest_covid_cases():
    # Sorting the data frame as per given criteria in descneding and selecting the top most record and state column
    highest_covid_cases=data.sort((data.confirm).cast("Long").desc()).select(col("state")).collect()[0][0]
    return jsonify({'get_highest_covid_cases':highest_covid_cases}) # returning the jsonfied response

@app.route('/least_covid_cases') # defing the things to happen on /least_covid_cases
def get_least_covid_cases():
    # Sorting the data frame as per given criteria in acending and selecting the top most record and state column
    least_covid_cases=data.sort(data.confirm.cast("Long")).select(col("state")).collect()[0][0]
    return jsonify({'get_least_covid_cases':least_covid_cases}) # returning the jsonfied response

@app.route('/total_cases') # defing the things to happen on /total_cases
def get_total_cases():
    # Suming the data frame as per given criteria and selecting the sum.
    total_cases=data.select(sum(data.confirm).alias("Total cases")).collect()[0][0]
    return jsonify({'Total Cases':total_cases}) # returning the jsonfied response
    
@app.route('/most_efficient_state') # defing the things to happen on /most_efficient_state
def get_most_efficient_state():
    # Sorting the data frame as per given criteria in descending and selecting the top most record and state column
    most_efficient_state=data.sort((data.cured.cast("Long")/data.confirm.cast("Long")).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most efficient_state':most_efficient_state}) # returning the jsonfied response

@app.route('/least_efficient_state') # defing the things to happen on /least_efficient_state
def get_least_efficient_state():
    # Sorting the data frame as per given criteria in ascending and selecting the top most record and state column
    least_efficient_state=data.sort((data.cured.cast("Long")/data.confirm.cast("Long")).asc()).select(col("state")).collect()[0][0]
    return jsonify({'least efficient_state':least_efficient_state}) # returning the jsonfied response

if __name__ == '__main__':
    app.run(debug=True) #running the app
