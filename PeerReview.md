## Shubham's Approach


### Question-1:
1. The get_data function sends a GET request to a specified URL with the required headers and retrieves JSON data containing COVID-19 information for India. The home route in the app.py file defines an endpoint that returns a JSON response with a menu of different options related to the COVID-19 data. It provides a user-friendly interface to access different functionalities, such as displaying API data, retrieving the most and least affected states, getting the state with the highest and lowest COVID-19 cases, and more.

### Question-2:
2.1: The /most_affected_state route in the Flask application executes a SQL query on a Spark DataFrame named COVID to retrieve the state with the highest ratio of total COVID-19 cases to total deaths, and returns the state name as a JSON response.

2.2: The /least_affected_state route in the Flask application executes a SQL query on a Spark DataFrame named COVID to retrieve the state with the lowest ratio of total COVID-19 cases to total deaths, and returns the state name as a JSON response.

2.3: The /highest_covid_cases route in the Flask application executes a SQL query on a Spark DataFrame named COVID to retrieve the state with the highest number of confirmed COVID-19 cases, and returns the state name as a JSON response.

2.4: The /least_covid_cases route in the Flask application executes a SQL query on a Spark DataFrame named COVID to retrieve the state with the lowest number of confirmed COVID-19 cases, and returns the state name as a JSON response.

2.5: The /total_cases route in the Flask application executes a SQL query on a Spark DataFrame named COVID to calculate the total number of confirmed COVID-19 cases across all states, and returns the total count as a JSON response.

2.6: The /handle_well route in the Flask application executes a SQL query on a Spark DataFrame named COVID to determine the state that has handled COVID-19 cases most efficiently, based on the ratio of total recoveries to total confirmed cases, and returns the state name and efficiency ratio as a JSON response.

2.7: The /least_well route in the Flask application returns the state with the lowest efficiency in handling COVID-19, which is calculated as the ratio of total recoveries to total COVID-19 cases. The function sorts the data in ascending order of this ratio and selects the state with the lowest value. It then returns the name of that state in JSON format.


## Shikhar's Approach:

### Question-1:
1. Used get_data() function to fetch the api data into response variable. Cleaned the data using popitem() function
2. The function create_csv takes a dictionary of state data and writes it to a CSV file. It writes the data row by row, including the header row with column names.
3. The createdataframefromcsv function reads the CSV file "output.csv" using Apache Spark, converts it into a DataFrame, and then prints the schema and displays the contents of the DataFrame. The function returns the DataFrame, which can be used for further data processing or analysis in subsequent functions.

### Question-2:
1. The make_csv function defines the route "/mk_csv_df" in the Flask application, which generates a CSV file from the API data with the last two records removed and returns a response indicating that the file has been created.
2. The show_api_data function defines the route "/show_api_data" in the Flask application, which sends a GET request to the specified API endpoint to fetch data related to COVID-19 cases in India, and returns the JSON response containing the data.
3. The get_most_affected_state function defines the route "/get_most_affected" in the Flask application. It checks if the "output.csv" file exists, and if not, it returns an error message. If the file exists, it creates a dataframe from the CSV file, creates a temporary view of the dataframe, executes an SQL query to calculate the ratio of death to confirmed cases for each state, orders the results in descending order, selects the state with the highest ratio, and returns it as a JSON response.
4. The get_least_affected_state function defines the route "/get_least_affected" in the Flask application. It checks if the "output.csv" file exists, and if not, it returns an error message. If the file exists, it creates a dataframe from the CSV file, creates a temporary view of the dataframe, executes an SQL query to calculate the ratio of death to confirmed cases for each state, orders the results in ascending order, selects the state with the lowest ratio, and returns it as a JSON response.
5. The highest_covid_cases function defines the route "/highest_covid_cases" in the Flask application. It checks if the "output.csv" file exists, and if not, it returns an error message. If the file exists, it creates a dataframe from the CSV file, creates a temporary view of the dataframe, orders the dataframe by the "confirm" column (confirmed covid cases) in descending order, selects the state and confirmed cases for the top record, and returns them as a JSON response.
6. The least_covid_cases function defines the route "/least_covid_cases" in the Flask application. It checks if the "output.csv" file exists, and if not, it returns an error message. If the file exists, it creates a dataframe from the CSV file, creates a temporary view of the dataframe, orders the dataframe by the "confirm" column (confirmed covid cases) in ascending order, selects the state and confirmed cases for the top record, and returns them as a JSON response.
7. The total_cases function defines the route "/total_cases" in the Flask application. It checks if the "output.csv" file exists, and if not, it returns an error message. If the file exists, it creates a dataframe from the CSV file, creates a temporary view of the dataframe, executes an SQL query using Spark SQL to calculate the sum of the "total" column, and returns the total cases as a JSON response.
8. The state_handle_well function defines the route "/handle_well" in the Flask application. It checks if the "output.csv" file exists, and if not, it returns an error message. If the file exists, it creates a dataframe from the CSV file, creates a temporary view of the dataframe, executes an SQL query using Spark SQL to calculate the efficiency of handling COVID cases for each state (cured cases divided by confirmed cases), orders the results in descending order of efficiency, and returns the top-most record (state with the highest efficiency) as a JSON response.
9. The state_least_well function defines the route "/least_well" in the Flask application. It checks if the "output.csv" file exists, and if not, it returns an error message. If the file exists, it creates a dataframe from the CSV file, creates a temporary view of the dataframe, executes an SQL query using Spark SQL to calculate the efficiency of handling COVID cases for each state (cured cases divided by confirmed cases), orders the results in ascending order of efficiency, selects the top-most record (state with the lowest efficiency), and returns it as a JSON response along with the corresponding efficiency value.
