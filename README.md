# Pinterest Data Pipeline
real time &amp; historical Pinterest data pipeline simulation

MAIN TECHNICAL WORK COMPLETED - MORE TO BE DONE ON DOCUMENTATION/REFACTOR

At minimum, your README file should contain the following information:

  X  Project Title
  ?  Table of Contents, if the README file is long
  x  A description of the project: what it does, the aim of the project, and what you learned
  x  Installation instructions
    Usage instructions
    File structure of the project

    The Pinterest Data Pipeline is an educational prototype for data gathering using common big data tooks such as Kafka, Spark, Airflow and Kinesis. Through the process most services were run locally, then implented in the cloud using Databricks and AWS.

## Architecture
<img src="https://raw.githubusercontent.com/DanBachmann/Pinterest_data_pipeline/main/documentation_resources/AiCore%20Pinterest%20Pipeline.png">
[Notebook view with output](DataBricks/Report_Request_Notebook_Output.html)
### Concepts learned/demonstrated
<ul>
<li>Using an EC2 instance via SSH, SSHFS and SCP.
<li>Local configuration of Kafka topics via command line.
<li>Basic MSK configuration in the AWS console.
<li>Use of Databricks to:
    <ul>
        <li>Connect to AWS S3 bucket
        <li>Load a series of files into a Spark dataframe
        <li>Connect to an AWS Kinesis data stream
        <li>Using PySpark to clean the data/streams and provide basic query results for static data
        <li>Store data transformed in DataBricks data lake
    </ul>
<li>Airflow in AWS - WMAA
    Of note is that Databricks runs extremely slow. This is apparently due to the shared nature of the AI Core account and the constant up/down sizing of instances. Despite this, it can be seen that syntax checking is generally quick and processing of dataframes appears to be lazy loaded as the serious delays only happen when displaying the data even if the dataframe only has a few rows.
</ul>

## Local Installation Components
<ul>
    <li>Java JRE 8
    <li>Kafka
    <li>AWS component for script access
</ul>


### Set-up scripts
With the security credentials filled in, in the set-up folder
<ul>
<li>setup_topics_and_connector.sh
</ul>

### Cloud requirements &amp;components
<ul>
    <li>AWS
        <ul>
            <li>S3
            <li>API Gateway
            <li>Kinesis



## Usage Instructions
<br>


### Optional Parameters
<hr>

[![License](https://img.shields.io/badge/License-Boost_1.0-lightblue.svg)](https://www.boost.org/LICENSE_1_0.txt)
