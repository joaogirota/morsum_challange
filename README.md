
# Question 1

This code is a Jupyter notebook to perform what was requested in task 1.

You can run the code by accessing: https://colab.research.google.com/drive/1mPyUp9F7t3irJ_7GXipceaeA7W5tSDem?usp=sharing

Add any .csv file link in the second cell. After that, click on all 'play' buttons in the order of the cells. First the first cell, then the second cell, and so on.

A function called 'parse_item_weight' was written, which only accepts dataframe objects. This function performs the requested treatment and returns the resulting dataframe.

A verification step was added to ensure that the treatment only occurs if the provided dataframe has the required columns.

# Question 2

A)

The basic idea behind synching two databases is to make a copy of the data that exists in one database A, in this case MySQL, to database B, which in this case is a data warehouse, ensuring that updates, deletions, or insertions in one of the databases are reflected in the other.

To do this, you can develop a script in some programming language (such as Python), which connects periodically to our MySQL database and ingests the data into our data warehouse, using a cloud function, for example.

Depending on the size and complexity of the data, an easier solution would be to use an ETL tool such as GCP's DataFlow to perform this data synching.

Since this database synching needs to be done regularly, I do recomend to configure a DAG in AirFlow that would execute this job in DataFlow at the specified interval.

1 - Configure a job in DataFlow. This can be done using a JSON or YAML file, for example.

2 - Create the data pipeline code (as I said, it can be Python, Java, or another language).

3 - Configure a DAG in AirFlow, importing the necessary libraries such as MySQL Operator, Python Operator, BigQuery Operator, and GCP Operator (to connect to DataFlow) and also the pipeline created.

4 - Define how often the DAG will be executed.

5 - Within the AirFlow DAG, insert the DataFlow job parameters. You can define the YAML path from step 1 or just create a variable within the DAG code itself to specify the path.

With this, at the specified time, AirFlow will execute the pipeline and synchronize the databases.

B)

i. It is more simple. Extract the data and use the pipeline to transform the data to match the BigQuery table schema, and then load it.

ii. It is more complex. When dealing with multiple tables, you must take care to correctly identify the keys and maintain the relationships. After that, the data can be transformed, paying attention to the schema, and ingested into their respective tables in BigQuery.

An important point to pay attention to is data consistency. There should always be a step to verify the data between MySQL and BigQuery to perform updates and deletions.

C)

i. Creating logs in the pipeline code is the most important thing, in my opinion. This will allow you to look at what happened within the code and what may have gone wrong.

In addition, it is possible to set up alerts in AirFlow for when the DAG or a job fails.

ii. It is always possible to manually execute the AirFlow DAG and perform data resynch. But I consider it more important:

1 - To have a backup of the data, in case the synch job fails and performs incorrect operations.

2 - The pipeline must have layers of data validation, so that possible errors are aborted and reported to the developer.

# Question 3

I added variables to a .toml file to improve security, and I also set the 'catchup' variable to TRUE, along with some other improvements.
