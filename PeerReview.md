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
