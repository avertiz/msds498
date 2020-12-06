# MSDS498 Capstone Project
This is the code repository for the capstone project for MSDS 498
The project was built on bothe AWS and Google Cloud Platform.

## Here is an overview of what each file accomplishes
#### requirements.txt
  * Packages needed for this project
#### Makefile
  * Basic scoffolding for environment setup and package installation
#### `test.py`
  * script for testing
#### `setupPostgreSQL.py`
* Demonstrates how the tables and schemas were setup which store the https://pushshift.io/ data and are access by all other processes
#### `etl.py`
  * This uses the https://pushshift.io/ API to get Reddit data from the Loan subreddit
  * The requests package is used to retrieve the data
  * AWS Lambda runs this script to automaticall get data
#### `eda.py`
  * EDA which ultimately determined how features would be created for the model
#### `features.py`
  * The script that created the features for the model
#### `create_feats.py`
  * This is how the features get generated on the fly which are used by the model to make online predictions 
#### `predict.py`
  * Script that houses the functions to produce online predictions from new pushshift data
#### `app.py`
  * The script that generated the web app
  * This is run on the Dash framework
#### `app.yaml`
  * basic yaml file that Google App Engine needs to deploy the app
  
#### Here are the cloud services that were used to create this app:
  * AWS RDS PostgreSQL
  * AWS Lambda
  * Google AutoML
  * Google App Engine 
