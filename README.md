# Pyspark-Assignment

## covid_data.py

```
def get_data():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"
    headers = {
        "X-RapidAPI-Key": KEY,
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers)
    return response

```

* This code defines a function called get_data() that retrieves COVID-19 data for India from a RapidAPI endpoint. It sets the URL of the endpoint and sets the headers to include the API key and host name. It then sends a GET request to the endpoint using the requests module in Python and returns the response. The response contains the COVID-19 data for India which can be used in further analysis or visualization.

```
def clean_data(data):
    data=data.drop('_corrupt_record')
    data=data.where((data.state.isNotNull()) & (data.state!=''))
    data = data.withColumn("confirm",col("confirm").cast("Long")).withColumn("cured",col("cured").cast("Long")).withColumn("death",col("death").cast("Long"))
    data=data.select("slno","state","confirm","cured","death","total")
    data=data.withColumn('state', regexp_replace('state', '\*',""))
    return 
```

* The clean_data function takes a dataframe data as input and performs several cleaning operations on it. Firstly, it drops the _corrupt_record column. Next, it filters out records where the state column is null or void. The function then casts the columns confirm, cured, and death to Long datatype. After that, it selects only the columns slno, state, confirm, cured, death, and total. Finally, it replaces any * character at the end of the state name with an empty string and returns the cleaned dataframe. Overall, the function ensures that the data is in the correct format and removes any unnecessary columns or rows.


```
spark = SparkSession.builder.master('local[*]').getOrCreate() 
sc = SparkContext.getOrCreate() 
sc.setLogLevel("ERROR")

response = get_data()

json_rdd = sc.parallelize(response.json().values()) 

data = spark.read.json(json_rdd) 
data = clean_data(data)
```

* First, we create a SparkSession object using the SparkSession.builder method and set it to run locally on all available cores. We also create a SparkContext object using SparkContext.getOrCreate() and set the log level to "ERROR" to avoid excessive output in the console.

* Next, we call the get_data() function to retrieve data from the COVID-19 India API. The function returns a response object, which we will use to create a dataframe.

* To create the dataframe, we first convert the JSON data in the response object into an RDD using the sc.parallelize() method. We then pass this RDD to the spark.read.json() method to create a dataframe.

* Finally, we pass the dataframe to the clean_data() function to perform some data cleaning operations, including dropping a column, filtering out null or empty state values, casting columns to the Long data type, and removing asterisks from the end of state names. The resulting cleaned dataframe will be used for further analysis.

## app.py:

```
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
```

* The Flask application route decorator function that returns a JSON object containing the descriptions of different endpoints of the API.

```
@app.route('/getcsvfile')
def getcsvfile():
    data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sonali_gudey/Desktop/spark_result")
    return jsonify({"Message":"Results stored succesfully to '/Users/sonali_gudey/Desktop/spark_result' path"})
```

* This is a Flask route that allows the user to export the data in the Spark dataframe to a CSV file. The data is first repartitioned into one partition and then saved as a CSV file with a header at the specified path. The function returns a JSON response confirming that the results were successfully stored at the given path.


#### Question:2

##### 2.1
```
@app.route('/most_affected_state')
def get_most_affected_state():
    most_affected_state = data.sort((data.death/data.confirm).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most_affected_state': most_affected_state})
```

* This code defines an endpoint in the Flask app that returns the state with the highest death-to-covid ratio. The Spark DataFrame is sorted by the death-to-covid ratio in descending order, and the state with the highest ratio is selected and returned in a JSON format.


##### 2.2
```
@app.route('/least_affected_state')
def get_least_affected_state():
    least_affected_state = data.sort((data.death/data.confirm)).select(col("state")).collect()[0][0]
    return jsonify({'least_affected_state': least_affected_state})
```

* This code defines an endpoint '/least_affected_state' which returns a JSON response containing the name of the state with the lowest death-to-covid ratio. It does this by sorting the dataframe 'data' by the death-to-covid ratio in ascending order, selecting the state column from the first row (i.e., the state with the lowest ratio), and returning it as a JSON object.


##### 2.3
```
@app.route('/highest_covid_cases')
def get_highest_covid_cases():
    highest_covid_cases = data.sort((data.confirm).desc()).select(col("state")).collect()[0][0]
    return jsonify({'get_highest_covid_cases':highest_covid_cases})
```

* This endpoint returns the state with the highest number of COVID cases in India. It sorts the data by the confirmed cases column in descending order, selects the state column, and returns the first row (which has the highest number of cases).


##### 2.4
```
@app.route('/least_covid_cases')
def get_least_covid_cases():
    least_covid_cases = data.sort(data.confirm).select(col("state")).collect()[0][0]
    return jsonify({'get_least_covid_cases':least_covid_cases})
```

* This code defines a route for the Flask app to get the state with the lowest number of COVID-19 cases. It sorts the Spark dataframe by the number of confirmed cases and selects the state with the lowest value. The state is then returned as a JSON object.


##### 2.5
```
@app.route('/total_cases')
def get_total_cases():
    total_cases = data.select(sum(data.confirm).alias("Total cases")).collect()[0][0]
    return jsonify({'Total Cases':total_cases})
```

* This endpoint returns the total number of COVID-19 cases in all Indian states combined. It does so by selecting the 'confirm' column from the cleaned Spark DataFrame, summing up all the values in this column and returning the result as a JSON object with the key 'Total Cases'.


##### 2.6
```
@app.route('/most_efficient_state')
def get_most_efficient_state():
    most_efficient_state = data.sort((data.cured/data.confirm).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most efficient_state':most_efficient_state})
```

* This function sorts the data by the ratio of total recoveries to total COVID cases, and returns the name of the state with the highest ratio. This state is considered to have handled the COVID situation most efficiently.


##### 2.7
```
@app.route('/least_efficient_state')
def get_least_efficient_state():
    least_efficient_state = data.sort((data.cured/data.confirm).asc()).select(col("state")).collect()[0][0]
    return jsonify({'least efficient_state':least_efficient_state})
```

* This function returns the state with the lowest efficiency in handling COVID-19, which is calculated as the ratio of total recoveries to total COVID-19 cases. The function sorts the data in ascending order of this ratio and selects the state with the lowest value. It then returns the name of that state in JSON format.

### Dataframe:

<img width="593" alt="Screenshot 2023-04-15 at 11 33 22 AM" src="https://user-images.githubusercontent.com/123619701/232187776-14c72436-fe4c-4033-be8f-7ba987fe3631.png">


<img width="911" alt="Screenshot 2023-04-15 at 11 37 21 AM" src="https://user-images.githubusercontent.com/123619701/232188664-b428cb8f-590a-43cd-b84c-1633e13e530f.png">

